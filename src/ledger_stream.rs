//! A Stream read whole from the Ledger: its length from its own record and
//! its chunks behind Xmip Storage, through the runtime's one chunk reader.

use std::io::Read;
use std::sync::Arc;

use persist::storage::XmipStorage;
use runtime::ledger::Chunks;
use xcore::StreamId;

/// The bytes of `stream`, read whole.
///
/// # Errors
///
/// In words, where its record or a chunk cannot be read, or what is read
/// is not its length.
pub fn read_whole(storage: &Arc<dyn XmipStorage>, stream: StreamId) -> Result<Vec<u8>, String> {
    let (length, content) = Chunks::referred(storage, stream).map_err(|e| e.to_string())?;
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(0));
    content
        .reader()
        .and_then(|mut reader| reader.read_to_end(&mut bytes))
        .map_err(|error| error.to_string())?;
    Ok(bytes)
}
