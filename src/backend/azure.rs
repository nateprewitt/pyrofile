#[cfg(feature = "azure")]
mod azure_impl {
    use std::sync::Arc;

    use azure_core::http::{NoFormat, RequestContent, XmlFormat};
    use azure_core::Bytes;
    use azure_storage_blob::models::{
        BlobClientDownloadOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    };
    use azure_storage_blob::{BlobClient, BlobClientOptions, BlockBlobClient, BlockBlobClientOptions};
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;

    use crate::backend::traits::{ObjectMeta, ObjectWriter, StorageBackend};
    use crate::error::{PyroError, Result};

    /// Azure Blob Storage backend.
    pub struct AzureBackend {
        blob_client: BlobClient,
        blob_url: url::Url,
        runtime: Arc<Runtime>,
        blob_url_str: String,
    }

    impl AzureBackend {
        const BLOCK_SIZE: usize = 32 * 1024 * 1024; // 32 MB
        const MAX_CONCURRENT_UPLOADS: usize = 8;
        const PARALLEL_READ_THRESHOLD: usize = 16 * 1024 * 1024; // 16 MB
        const READ_PARTITION_SIZE: usize = 16 * 1024 * 1024; // 16 MB
        const MAX_CONCURRENT_READS: usize = 8;

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
                blob_url: parsed_url,
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

        /// Single range GET.
        fn download_range(&self, offset: u64, length: usize) -> Result<Bytes> {
            let mut options = BlobClientDownloadOptions::default();
            options.range = Some(format!("bytes={}-{}", offset, offset + length as u64 - 1));

            self.block_on_safe(async {
                let response = self
                    .blob_client
                    .download(Some(options))
                    .await
                    .map_err(|e| PyroError::Backend(format!("download error: {e}")))?;
                response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| PyroError::Backend(format!("read body error: {e}")))
            })
        }

        /// Parallel chunked download. Returns positioned chunks.
        fn download_chunked(&self, offset: u64, length: usize) -> Result<Vec<(usize, Bytes)>> {
            let partition_size = Self::READ_PARTITION_SIZE;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(Self::MAX_CONCURRENT_READS));
            let blob_url = self.blob_url.clone();

            let results: Vec<std::result::Result<(usize, Bytes), PyroError>> =
                self.block_on_safe(async {
                    let mut handles = Vec::new();
                    let mut pos = 0usize;

                    while pos < length {
                        let chunk_len = partition_size.min(length - pos);
                        let chunk_offset = offset + pos as u64;
                        let buf_pos = pos;
                        let url = blob_url.clone();
                        let sem = Arc::clone(&semaphore);

                        handles.push(tokio::spawn(async move {
                            let _permit = sem.acquire().await.map_err(|e| {
                                PyroError::Backend(format!("semaphore error: {e}"))
                            })?;

                            let client = BlobClient::from_url(
                                url,
                                None::<Arc<dyn azure_core::credentials::TokenCredential>>,
                                None,
                            )
                            .map_err(|e| PyroError::Backend(format!("client error: {e}")))?;

                            let mut options = BlobClientDownloadOptions::default();
                            options.range = Some(format!(
                                "bytes={}-{}",
                                chunk_offset,
                                chunk_offset + chunk_len as u64 - 1
                            ));

                            let response =
                                client.download(Some(options)).await.map_err(|e| {
                                    PyroError::Backend(format!("download error: {e}"))
                                })?;
                            let body: Bytes =
                                response.into_body().collect().await.map_err(|e| {
                                    PyroError::Backend(format!("read body error: {e}"))
                                })?;

                            Ok::<(usize, Bytes), PyroError>((buf_pos, body))
                        }));

                        pos += chunk_len;
                    }

                    let mut results = Vec::with_capacity(handles.len());
                    for handle in handles {
                        results.push(
                            handle.await.map_err(|e| {
                                PyroError::Backend(format!("task join error: {e}"))
                            })?,
                        );
                    }
                    Ok::<_, PyroError>(results)
                })?;

            results.into_iter().collect()
        }

        fn assemble_chunks(chunks: Vec<(usize, Bytes)>, length: usize) -> Vec<u8> {
            let mut out = vec![0u8; length];
            for (pos, data) in chunks {
                let n = data.len().min(length - pos);
                out[pos..pos + n].copy_from_slice(&data[..n]);
            }
            out
        }
    }

    impl StorageBackend for AzureBackend {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }

            let data = if buf.len() < Self::PARALLEL_READ_THRESHOLD {
                self.download_range(offset, buf.len())?
            } else {
                let chunks = self.download_chunked(offset, buf.len())?;
                Bytes::from(Self::assemble_chunks(chunks, buf.len()))
            };

            let n = data.len().min(buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            Ok(n)
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

        fn download(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
            if length == 0 {
                return Ok(Vec::new());
            }

            let len = length as usize;
            if len < Self::PARALLEL_READ_THRESHOLD {
                let data = self.download_range(offset, len)?;
                Ok(data.to_vec())
            } else {
                let chunks = self.download_chunked(offset, len)?;
                Ok(Self::assemble_chunks(chunks, len))
            }
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

        fn wait_for_in_flight(&mut self) -> Result<()> {
            let handles: Vec<_> = self.in_flight.drain(..).collect();
            for handle in handles {
                self.runtime
                    .block_on(handle)
                    .map_err(|e| PyroError::Backend(format!("task join error: {e}")))??;
            }
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
            self.wait_for_in_flight()
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
