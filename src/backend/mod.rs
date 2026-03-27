pub mod traits;
pub mod smart_writer;
pub mod local;
#[cfg(feature = "azure")]
pub mod azure;

#[allow(unused_imports)]
pub use traits::{CompletedPart, ObjectMeta, ObjectWriter, StorageBackend, UploadPrimitives};
#[allow(unused_imports)]
pub use smart_writer::SmartWriter;
#[allow(unused_imports)]
pub use local::{LocalBackend, LocalWriter};
#[cfg(feature = "azure")]
pub use azure::{AzureBackend, AzureWriter};
