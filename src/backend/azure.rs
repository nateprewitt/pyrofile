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
    use tokio::task::JoinHandle;

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
        fn copy_blocks_parallel(
            data: &[u8],
            block_size: usize,
            max_workers: usize,
        ) -> Vec<Vec<u8>> {
            let block_count = data.len() / block_size;
            let worker_count = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(max_workers)
                .min(block_count);

            if worker_count <= 1 {
                return data
                    .chunks_exact(block_size)
                    .map(<[u8]>::to_vec)
                    .collect();
            }

            let blocks_per_worker = block_count.div_ceil(worker_count);
            let worker_bytes = blocks_per_worker * block_size;

            std::thread::scope(|scope| {
                let copies: Vec<_> = data
                    .chunks(worker_bytes)
                    .map(|blocks| {
                        scope.spawn(move || {
                            blocks
                                .chunks_exact(block_size)
                                .map(<[u8]>::to_vec)
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();

                copies
                    .into_iter()
                    .flat_map(|copy| copy.join().expect("block copy worker panicked"))
                    .collect()
            })
        }

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

            // Copy full blocks in parallel, one upload window at a time.
            let full_block_bytes =
                remaining.len() / self.config.part_size * self.config.part_size;
            let (full_blocks, tail) = remaining.split_at(full_block_bytes);
            let copy_window = self
                .config
                .part_size
                .checked_mul(self.config.max_concurrent_uploads.max(1))
                .unwrap_or(full_blocks.len().max(self.config.part_size));

            for window in full_blocks.chunks(copy_window) {
                for block in Self::copy_blocks_parallel(
                    window,
                    self.config.part_size,
                    self.config.max_concurrent_uploads.max(1),
                ) {
                    self.spawn_block_upload(block)?;
                }
            }
            remaining = tail;

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

    #[cfg(test)]
    mod tests {
        use super::AzureWriter;

        #[test]
        fn parallel_block_copy_preserves_order() {
            let data: Vec<u8> = (0..48).collect();
            let blocks = AzureWriter::copy_blocks_parallel(&data, 16, 2);

            assert_eq!(blocks.len(), 3);
            assert_eq!(blocks.concat(), data);
        }

        #[test]
        fn parallel_block_copy_ignores_partial_tail() {
            let data: Vec<u8> = (0..40).collect();
            let blocks = AzureWriter::copy_blocks_parallel(&data, 16, 2);

            assert_eq!(blocks.len(), 2);
            assert_eq!(blocks.concat(), data[..32]);
        }
    }
}

#[cfg(feature = "azure")]
pub use azure_impl::{AzureBackend, AzureWriter};
