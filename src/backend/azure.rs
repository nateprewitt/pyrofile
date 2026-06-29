#[cfg(feature = "azure")]
mod azure_impl {
    use std::sync::{Arc, Mutex};

    use azure_core::http::{NoFormat, RequestContent, XmlFormat};
    use azure_core::Bytes;
    use azure_storage_blob::models::{
        BlobClientDownloadOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    };
    use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient};
    use futures::StreamExt;
    use tokio::runtime::Runtime;
    use tokio::task::JoinHandle;

    use crate::backend::traits::{ObjectMeta, ObjectWriter, StorageBackend};
    use crate::error::{PyroError, Result};

    type SharedCredential = Arc<dyn azure_core::credentials::TokenCredential>;

    const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
    const CREDENTIAL_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

    /// Process-wide tokio runtime + Azure credential.
    struct SharedAzure {
        pid: u32,
        runtime: Arc<Runtime>,
        credential: Option<SharedCredential>,
    }

    static SHARED_AZURE: Mutex<Option<SharedAzure>> = Mutex::new(None);

    fn build_runtime() -> Result<Arc<Runtime>> {
        Runtime::new()
            .map(Arc::new)
            .map_err(|e| PyroError::Backend(format!("tokio runtime error: {e}")))
    }

    /// Choose a credential for the current host, preferring the following:
    ///   1. Environment service principal
    ///   2. AKS workload identity
    ///   3. Managed identity
    ///   4. Developer tools
    fn select_credential(runtime: &Runtime) -> Result<SharedCredential> {
        use azure_core::credentials::TokenCredential;

        // Environment variables
        if let (Ok(tenant_id), Ok(client_id), Ok(secret)) = (
            std::env::var("AZURE_TENANT_ID"),
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_CLIENT_SECRET"),
        ) {
            if let Ok(cred) = azure_identity::ClientSecretCredential::new(
                &tenant_id,
                client_id,
                secret.into(),
                None,
            ) {
                let cred: SharedCredential = cred;
                return Ok(cred);
            }
        }

        // AKS workload identity
        if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() {
            if let Ok(cred) = azure_identity::WorkloadIdentityCredential::new(None) {
                let cred: SharedCredential = cred;
                return Ok(cred);
            }
        }

        // Managed identity
        if let Ok(managed) = azure_identity::ManagedIdentityCredential::new(None) {
            let usable = runtime.block_on(async {
                tokio::time::timeout(
                    CREDENTIAL_PROBE_TIMEOUT,
                    managed.get_token(&[STORAGE_SCOPE], None),
                )
                .await
                .map(|inner| inner.is_ok())
                .unwrap_or(false)
            });
            if usable {
                let cred: SharedCredential = managed;
                return Ok(cred);
            }
        }

        // Developer tools
        let cred: SharedCredential = azure_identity::DeveloperToolsCredential::new(None)
            .map_err(|e| PyroError::Backend(format!("credential error: {e}")))?;
        Ok(cred)
    }

    /// Return the process-wide runtime and, when `need_credential` is set, the
    /// shared credential.
    fn shared_azure(need_credential: bool) -> Result<(Arc<Runtime>, Option<SharedCredential>)> {
        let mut guard = SHARED_AZURE
            .lock()
            .map_err(|e| PyroError::Backend(format!("shared azure lock poisoned: {e}")))?;

        let pid = std::process::id();
        let forked = guard.as_ref().map_or(true, |shared| shared.pid != pid);
        if forked {
            if let Some(stale) = guard.take() {
                // Inherited from a parent process across fork(): leak, don't drop.
                std::mem::forget(stale);
            }
            *guard = Some(SharedAzure {
                pid,
                runtime: build_runtime()?,
                credential: None,
            });
        }

        let shared = guard.as_mut().expect("shared azure built above");
        if need_credential && shared.credential.is_none() {
            shared.credential = Some(select_credential(&shared.runtime)?);
        }

        Ok((Arc::clone(&shared.runtime), shared.credential.clone()))
    }

    /// Azure Blob Storage backend.
    pub struct AzureBackend {
        blob_client: Arc<BlobClient>,
        runtime: Arc<Runtime>,
        blob_url_str: String,
    }

    impl AzureBackend {
        /// Create a new AzureBackend from a full blob URL.
        pub fn new(blob_url: &str) -> Result<Self> {
            let parsed_url = url::Url::parse(blob_url)
                .map_err(|e| PyroError::InvalidArgument(format!("invalid URL: {e}")))?;

            // A SAS token (sig= in query) authenticates anonymously, so we don't
            // need (or want to build) a credential for it.
            let is_sas = parsed_url.query().map_or(false, |q| q.contains("sig="));
            let (runtime, shared_credential) = shared_azure(!is_sas)?;
            let credential: Option<SharedCredential> = if is_sas { None } else { shared_credential };

            let blob_client = BlobClient::from_url(
                parsed_url.clone(),
                credential,
                Some(BlobClientOptions::default()),
            )
            .map_err(|e| PyroError::Backend(format!("client error: {e}")))?;

            Ok(Self {
                blob_client: Arc::new(blob_client),
                runtime,
                blob_url_str: blob_url.to_string(),
            })
        }

        fn block_on_safe<F>(&self, future: F) -> F::Output
        where
            F: std::future::Future + Send,
            F::Output: Send,
        {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => std::thread::scope(|s| {
                    s.spawn(|| handle.block_on(future)).join().unwrap()
                }),
                Err(_) => self.runtime.block_on(future),
            }
        }

        /// Single range GET, streamed directly into the provided buffer.
        fn download_into(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let mut options = BlobClientDownloadOptions::default();
            options.range = Some(format!("bytes={}-{}", offset, offset + buf.len() as u64 - 1));

            self.block_on_safe(async {
                let response = self
                    .blob_client
                    .download(Some(options))
                    .await
                    .map_err(|e| PyroError::Backend(format!("download error: {e}")))?;

                let mut body = response.into_body();
                let mut filled = 0usize;

                while let Some(chunk) = body.next().await {
                    let chunk = chunk
                        .map_err(|e| PyroError::Backend(format!("read body error: {e}")))?;
                    let n = chunk.len().min(buf.len() - filled);
                    buf[filled..filled + n].copy_from_slice(&chunk[..n]);
                    filled += n;
                    if filled >= buf.len() {
                        break;
                    }
                }

                Ok(filled)
            })
        }
    }

    impl StorageBackend for AzureBackend {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            self.download_into(offset, buf)
        }

        fn read_ranges(&self, ranges: &[(u64, usize)], dest: &mut [u8], max_concurrency: usize) -> Result<usize> {
            if ranges.is_empty() {
                return Ok(0);
            }

            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrency));

            let mut dest_offset = 0usize;

            self.block_on_safe(async {
                let mut futs = futures::stream::FuturesUnordered::new();

                for &(file_offset, len) in ranges {
                    let client = Arc::clone(&self.blob_client);
                    let sem = Arc::clone(&semaphore);
                    let buf_pos = dest_offset;
                    dest_offset += len;

                    futs.push(tokio::spawn(async move {
                        let _permit = sem.acquire().await.map_err(|e| {
                            PyroError::Backend(format!("semaphore error: {e}"))
                        })?;

                        let mut options = BlobClientDownloadOptions::default();
                        options.range = Some(format!(
                            "bytes={}-{}",
                            file_offset,
                            file_offset + len as u64 - 1,
                        ));

                        let response = client
                            .download(Some(options))
                            .await
                            .map_err(|e| PyroError::Backend(format!("download error: {e}")))?;

                        let data: Bytes = response
                            .into_body()
                            .collect()
                            .await
                            .map_err(|e| PyroError::Backend(format!("read body error: {e}")))?;

                        Ok::<(usize, Bytes), PyroError>((buf_pos, data))
                    }));
                }

                let mut filled = 0usize;
                while let Some(result) = futures::StreamExt::next(&mut futs).await {
                    let (buf_pos, data) = result
                        .map_err(|e| PyroError::Backend(format!("task join error: {e}")))??;
                    let n = data.len().min(dest.len() - buf_pos);
                    dest[buf_pos..buf_pos + n].copy_from_slice(&data[..n]);
                    filled += n;
                }
                Ok::<usize, PyroError>(filled)
            })
        }

        fn metadata(&self) -> Result<ObjectMeta> {
            use azure_storage_blob::models::BlobClientGetPropertiesResultHeaders;
            let props = self.block_on_safe(async {
                self.blob_client
                    .get_properties(None)
                    .await
                    .map_err(|e| PyroError::Backend(format!("get_properties error: {e}")))
            })?;

            let content_length = props
                .content_length()
                .map_err(|e| PyroError::Backend(format!("content_length header error: {e}")))?;

            Ok(ObjectMeta {
                content_length,
                content_type: None,
            })
        }

        fn create_writer(&self) -> Result<Box<dyn ObjectWriter>> {
            let block_blob_client = self.blob_client.block_blob_client();
            let config = crate::core::config::WriteConfig::default();
            Ok(Box::new(AzureWriter {
                block_blob_client: Arc::new(block_blob_client),
                runtime: Arc::clone(&self.runtime),
                buffer: Vec::new(),
                block_ids: Vec::new(),
                in_flight: Vec::new(),
                config,
                closed: false,
            }))
        }

        fn name(&self) -> &str {
            &self.blob_url_str
        }
    }

    /// Azure writer with parallel block uploads.
    pub struct AzureWriter {
        block_blob_client: Arc<BlockBlobClient>,
        runtime: Arc<Runtime>,
        buffer: Vec<u8>,
        block_ids: Vec<Vec<u8>>,
        in_flight: Vec<JoinHandle<Result<()>>>,
        config: crate::core::config::WriteConfig,
        closed: bool,
    }

    impl AzureWriter {
        fn spawn_block_upload(&mut self, data: Vec<u8>) -> Result<()> {
            // Apply backpressure: don't spawn if at concurrency cap.
            if self.in_flight.len() >= self.config.max_concurrent_uploads {
                self.drain_completed()?;
            }
            if self.in_flight.len() >= self.config.max_concurrent_uploads {
                self.wait_for_one()?;
            }

            let block_id = uuid::Uuid::new_v4().to_string().into_bytes();
            self.block_ids.push(block_id.clone());

            let client = Arc::clone(&self.block_blob_client);
            let content_length = data.len() as u64;

            let handle = self.runtime.spawn(async move {
                let body: RequestContent<Bytes, NoFormat> = Bytes::from(data).into();

                client
                    .stage_block(
                        &block_id,
                        content_length,
                        body,
                        None::<BlockBlobClientStageBlockOptions<'_>>,
                    )
                    .await
                    .map_err(|e| PyroError::Backend(format!("stage_block error: {e}")))?;

                Ok(())
            });

            self.in_flight.push(handle);
            Ok(())
        }

        fn drain_completed(&mut self) -> Result<()> {
            let mut still_running = Vec::new();
            for handle in self.in_flight.drain(..) {
                if handle.is_finished() {
                    self.runtime
                        .block_on(handle)
                        .map_err(|e| PyroError::Backend(format!("task join error: {e}")))??;
                } else {
                    still_running.push(handle);
                }
            }
            self.in_flight = still_running;
            Ok(())
        }

        fn wait_for_one(&mut self) -> Result<()> {
            if self.in_flight.is_empty() {
                return Ok(());
            }
            let handle = self.in_flight.remove(0);
            self.runtime
                .block_on(handle)
                .map_err(|e| PyroError::Backend(format!("task join error: {e}")))??;
            Ok(())
        }

        fn wait_for_in_flight(&mut self) -> Result<()> {
            let handles: Vec<_> = self.in_flight.drain(..).collect();
            if handles.is_empty() {
                return Ok(());
            }
            self.runtime.block_on(async {
                for handle in handles {
                    handle.await
                        .map_err(|e| PyroError::Backend(format!("task join error: {e}")))??;
                }
                Ok::<(), PyroError>(())
            })?;
            Ok(())
        }
    }

    impl ObjectWriter for AzureWriter {
        fn write(&mut self, data: &[u8]) -> Result<()> {
            if self.closed {
                return Err(PyroError::Closed);
            }

            let mut remaining = data;

            // Fill any partial buffer from a previous small write.
            if !self.buffer.is_empty() {
                let need = self.config.part_size - self.buffer.len();
                let take = remaining.len().min(need);
                self.buffer.extend_from_slice(&remaining[..take]);
                remaining = &remaining[take..];

                if self.buffer.len() >= self.config.part_size {
                    let block = std::mem::take(&mut self.buffer);
                    self.spawn_block_upload(block)?;
                }
            }

            // Upload full blocks directly from input.
            while remaining.len() >= self.config.part_size {
                let block = remaining[..self.config.part_size].to_vec();
                remaining = &remaining[self.config.part_size..];
                self.spawn_block_upload(block)?;
            }

            // Buffer any sub-block-size tail.
            if !remaining.is_empty() {
                self.buffer.extend_from_slice(remaining);
            }

            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            self.drain_completed()
        }

        fn close(&mut self) -> Result<()> {
            if self.closed {
                return Ok(());
            }

            if !self.buffer.is_empty() {
                let data = std::mem::take(&mut self.buffer);
                self.spawn_block_upload(data)?;
            }

            self.wait_for_in_flight()?;

            let block_list = BlockLookupList {
                committed: None,
                uncommitted: Some(self.block_ids.clone()),
                latest: None,
            };

            let content: RequestContent<BlockLookupList, XmlFormat> = block_list
                .try_into()
                .map_err(|e: azure_core::Error| {
                    PyroError::Backend(format!("block list serialization error: {e}"))
                })?;

            self.runtime
                .block_on(async {
                    self.block_blob_client
                        .commit_block_list(content, None)
                        .await
                        .map_err(|e| {
                            PyroError::Backend(format!("commit_block_list error: {e}"))
                        })
                })?;

            self.closed = true;
            Ok(())
        }

        fn abort(&mut self) -> Result<()> {
            self.closed = true;
            self.in_flight.clear();
            self.buffer.clear();
            self.block_ids.clear();
            Ok(())
        }
    }

    impl Drop for AzureWriter {
        fn drop(&mut self) {
            if !self.closed {
                let _ = self.abort();
            }
        }
    }
}

#[cfg(feature = "azure")]
pub use azure_impl::{AzureBackend, AzureWriter};
