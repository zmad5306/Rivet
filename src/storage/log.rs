use crate::storage::record::Record;
use crate::{error::StorageError, storage::record::RecordLimits};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub struct Log {
    file: File,
    limits: RecordLimits,
}

impl Log {
    pub fn open(path: &Path, limits: RecordLimits) -> Result<Self, StorageError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Log { file, limits })
    }

    pub fn append(&mut self, record: &Record) -> Result<(), StorageError> {
        let bytes = record.encode(&self.limits)?;
        self.file.write_all(&bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}
