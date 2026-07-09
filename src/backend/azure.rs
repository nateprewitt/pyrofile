#[cfg(feature = "azure")]
mod azure_impl {
    use std::sync::{Arc, Mutex};

    use azure_core::http::{
        new_http_client, HttpClient, NoFormat, RequestContent, Transport, XmlFormat,
    };
    use azure_core::Bytes;
    use azure_storage_blob::models::{
        BlobClientDownloadOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    };
    use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient};
    use futures::StreamExt;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;

    use crate::backend::traits::{ObjectMeta, ObjectWriter, StorageBackend};
    use crate::error::{PyroError, Result};

    type SharedCredential = Arc<dyn azure_core::credentials::TokenCredential>;
    type SharedHttpClient = Arc<dyn HttpClient>;

    const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
    const CREDENTIAL_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

    /// Process-wide tokio runtime, HTTP client, and Azure credential.
    struct SharedAzure {
        pid: u32,
        runtime: Arc<Runtime>,
        http: SharedHttpClient,
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

        // 4. Developer tools (Azure CLI / azd).
        let cred: SharedCredential = azure_identity::DeveloperToolsCredential::new(None)
            .map_err(|e| PyroError::Backend(format!("credential error: {e}")))?;
        Ok(cred)
    }

    fn shared_azure(
        need_credential: bool,
    ) -> Result<(Arc<Runtime>, SharedHttpClient, Option<SharedCredential>)> {
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
                http: new_http_client(),
                credential: None,
            });
        }

        let shared = guard.as_mut().expect("shared azure built above");
        if need_credential && shared.credential.is_none() {
            shared.credential = Some(select_credential(&shared.runtime)?);
        }

        Ok((
            Arc::clone(&shared.runtime),
            Arc::clone(&shared.http),
            shared.credential.clone(),
        ))
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
            let (runtime, http, shared_credential) = shared_azure(!is_sas)?;
            let credential: Option<SharedCredential> = if is_sas { None } else { shared_credential };

            let mut options = BlobClientOptions::default();
            options.client_options.transport = Some(Transport::new(http));

            let blob_client = BlobClient::from_url(parsed_url.clone(), credential, Some(options))
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
                let response = match self.blob_client.download(Some(options)).await {
                    Ok(r) => r,
                    Err(e) => {
                        // A ranged GET against an empty blob (or a range starting
                        // past EOF) returns HTTP 416. Treat it as a clean
                        // EOF (zero bytes) rather than surfacing an error.
                        if e.http_status()
                            == Some(azure_core::http::StatusCode::RequestedRangeNotSatisfiable)
                        {
                            return Ok(0usize);
                        }
                        return Err(PyroError::Backend(format!("download error: {e}")));
                    }
                };

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

    /// Parse the total object length out of a `Content-Range` header value.
    /// Returns `None` when the total is unknown (`"*"`) or the header is malformed.
    fn parse_content_range_total(range: &str) -> Option<u64> {
        range.rsplit('/').next()?.trim().parse::<u64>().ok()
    }

    impl StorageBackend for AzureBackend {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            self.download_into(offset, buf)
        }

        fn read_chunk_sized(&self, offset: u64, max_len: usize) -> Result<(Vec<u8>, Option<u64>)> {
            use azure_storage_blob::models::BlobClientDownloadResultHeaders;

            if max_len == 0 {
                return Ok((Vec::new(), None));
            }

            let mut options = BlobClientDownloadOptions::default();
            options.range = Some(format!("bytes={}-{}", offset, offset + max_len as u64 - 1));

            self.block_on_safe(async {
                let response = match self.blob_client.download(Some(options)).await {
                    Ok(r) => r,
                    Err(e) => {
                        if e.http_status()
                            == Some(azure_core::http::StatusCode::RequestedRangeNotSatisfiable)
                        {
                            return Ok((Vec::new(), None));
                        }
                        return Err(PyroError::Backend(format!("download error: {e}")));
                    }
                };

                // Harvest the total object size from `Content-Range` header before read
                let total = response
                    .content_range()
                    .ok()
                    .flatten()
                    .as_deref()
                    .and_then(parse_content_range_total);

                let data: Bytes = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| PyroError::Backend(format!("read body error: {e}")))?;

                Ok((data.to_vec(), total))
            })
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
            let max = config.max_concurrent_uploads;
            Ok(Box::new(AzureWriter {
                block_blob_client: Arc::new(block_blob_client),
                runtime: Arc::clone(&self.runtime),
                buffer: Vec::new(),
                block_ids: Vec::new(),
                sem: Arc::new(Semaphore::new(max)),
                err: Arc::new(Mutex::new(None)),
                max_concurrency: max as u32,
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
        /// Bounds in-flight uploads; a permit is held for the lifetime of each
        /// spawned task, so dispatch resumes when *any* upload completes.
        sem: Arc<Semaphore>,
        /// First error observed by a background upload task, if any.
        err: Arc<Mutex<Option<PyroError>>>,
        max_concurrency: u32,
        config: crate::core::config::WriteConfig,
        closed: bool,
    }

    impl AzureWriter {
        fn spawn_block_upload(&mut self, data: Bytes) -> Result<()> {
            // Surface any earlier upload error before queueing more work.
            self.check_error()?;

            // Backpressure: block only until a permit is free (i.e. any
            // in-flight upload has finished), not on a specific handle.
            let permit = self
                .runtime
                .block_on(Arc::clone(&self.sem).acquire_owned())
                .map_err(|e| PyroError::Backend(format!("semaphore error: {e}")))?;

            let block_id = uuid::Uuid::new_v4().to_string().into_bytes();
            self.block_ids.push(block_id.clone());

            let client = Arc::clone(&self.block_blob_client);
            let content_length = data.len() as u64;
            let err_slot = Arc::clone(&self.err);

            self.runtime.spawn(async move {
                // Held for the whole upload; released here, unblocking dispatch.
                let _permit = permit;
                let body: RequestContent<Bytes, NoFormat> = data.into();

                let result = client
                    .stage_block(
                        &block_id,
                        content_length,
                        body,
                        None::<BlockBlobClientStageBlockOptions<'_>>,
                    )
                    .await
                    .map_err(|e| PyroError::Backend(format!("stage_block error: {e}")));

                if let Err(e) = result {
                    let mut slot = err_slot.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                }
            });

            Ok(())
        }

        /// Take and return the first background error, if one occurred.
        fn check_error(&self) -> Result<()> {
            if let Some(e) = self.err.lock().unwrap().take() {
                return Err(e);
            }
            Ok(())
        }

        /// Wait for every in-flight upload to finish, then surface any error.
        /// All permits are free exactly when no upload is running.
        fn wait_for_all(&mut self) -> Result<()> {
            let sem = Arc::clone(&self.sem);
            let max = self.max_concurrency;
            self.runtime.block_on(async move {
                sem.acquire_many(max)
                    .await
                    .map_err(|e| PyroError::Backend(format!("semaphore error: {e}")))
                    .map(|_permits| ())
            })?;
            self.check_error()
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
                    self.spawn_block_upload(Bytes::from(block))?;
                }
            }

            // Upload full blocks, copying each one out just before dispatch so
            // the copy overlaps with in-flight uploads (avoid buffering the
            // whole write up front).
            while remaining.len() >= self.config.part_size {
                let block = remaining[..self.config.part_size].to_vec();
                remaining = &remaining[self.config.part_size..];
                self.spawn_block_upload(Bytes::from(block))?;
            }

            // Buffer any sub-block-size tail.
            if !remaining.is_empty() {
                self.buffer.extend_from_slice(remaining);
            }

            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            self.check_error()
        }

        fn close(&mut self) -> Result<()> {
            if self.closed {
                return Ok(());
            }

            if !self.buffer.is_empty() {
                let data = std::mem::take(&mut self.buffer);
                self.spawn_block_upload(Bytes::from(data))?;
            }

            self.wait_for_all()?;

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
