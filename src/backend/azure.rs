#[cfg(feature = "azure")]
mod azure_impl {
    use std::sync::Arc;

    use azure_core::http::{NoFormat, RequestContent, XmlFormat};
    use azure_core::Bytes;
    use azure_storage_blob::models::{
        BlobClientDownloadOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    };
    use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient};
    use futures::StreamExt;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;

    use crate::backend::traits::{ObjectMeta, ObjectWriter, StorageBackend};
    use crate::error::{PyroError, Result};

    /// Azure Blob Storage backend.
    pub struct AzureBackend {
        blob_client: BlobClient,
        runtime: Arc<Runtime>,
        blob_url_str: String,
    }

    impl AzureBackend {
        const BLOCK_SIZE: usize = 16 * 1024 * 1024; // 16 MB
        const MAX_CONCURRENT_UPLOADS: usize = 32;

        /// Create a new AzureBackend from a full blob URL.
        /// Uses DeveloperToolsCredential for authentication.
        /// If the URL contains a SAS token, pass `None` as credential.
        pub fn new(blob_url: &str) -> Result<Self> {
            let parsed_url = url::Url::parse(blob_url)
                .map_err(|e| PyroError::InvalidArgument(format!("invalid URL: {e}")))?;

            // If URL has a SAS token (sig= in query), use anonymous auth.
            // Otherwise, use DeveloperToolsCredential.
            let credential: Option<Arc<dyn azure_core::credentials::TokenCredential>> =
                if parsed_url.query().map_or(false, |q| q.contains("sig=")) {
                    None
                } else {
                    let cred = azure_identity::DeveloperToolsCredential::new(None)
                        .map_err(|e| PyroError::Backend(format!("credential error: {e}")))?;
                    Some(cred)
                };

            let blob_client = BlobClient::from_url(
                parsed_url.clone(),
                credential,
                Some(BlobClientOptions::default()),
            )
            .map_err(|e| PyroError::Backend(format!("client error: {e}")))?;

            let runtime = Runtime::new()
                .map_err(|e| PyroError::Backend(format!("tokio runtime error: {e}")))?;

            Ok(Self {
                blob_client,
                runtime: Arc::new(runtime),
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
            Ok(Box::new(AzureWriter {
                block_blob_client: Arc::new(block_blob_client),
                runtime: Arc::clone(&self.runtime),
                buffer: Vec::new(),
                block_ids: Vec::new(),
                in_flight: Vec::new(),
                semaphore: Arc::new(Semaphore::new(Self::MAX_CONCURRENT_UPLOADS)),
                block_size: Self::BLOCK_SIZE,
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
        semaphore: Arc<Semaphore>,
        block_size: usize,
        closed: bool,
    }

    impl AzureWriter {
        fn spawn_block_upload(&mut self, data: Vec<u8>) -> Result<()> {
            let block_id = uuid::Uuid::new_v4().to_string().into_bytes();
            self.block_ids.push(block_id.clone());

            let client = Arc::clone(&self.block_blob_client);
            let semaphore = Arc::clone(&self.semaphore);
            let content_length = data.len() as u64;

            let handle = self.runtime.spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| PyroError::Backend(format!("semaphore error: {e}")))?;

                let body: RequestContent<Bytes, NoFormat> = Bytes::from(data).into();

                let start = std::time::Instant::now();
                client
                    .stage_block(
                        &block_id,
                        content_length,
                        body,
                        None::<BlockBlobClientStageBlockOptions<'_>>,
                    )
                    .await
                    .map_err(|e| PyroError::Backend(format!("stage_block error: {e}")))?;
                eprintln!("[stage_block] size={content_length} elapsed={:?}", start.elapsed());

                Ok(())
            });

            self.in_flight.push(handle);
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
            self.buffer.extend_from_slice(data);

            while self.buffer.len() >= self.block_size {
                let remainder = self.buffer.split_off(self.block_size);
                let block_data = std::mem::replace(&mut self.buffer, remainder);
                self.spawn_block_upload(block_data)?;
            }

            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            // Surface errors from finished transfers early.
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
