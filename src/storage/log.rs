use crate::error::CodecError;
use crate::storage::record::HEADER_LENGTH;
use crate::storage::record::Record;
use crate::{error::StorageError, storage::record::RecordLimits};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, Read, Write};
use std::path::Path;

pub struct LogScanner<'a> {
    reader: BufReader<File>,
    limits: &'a RecordLimits,
    finished: bool,
}

impl LogScanner<'_> {
    pub fn read_next_record(&mut self) -> Result<Option<Record>, StorageError> {
        if self.finished {
            return Ok(None);
        }

        let mut header = [0u8; HEADER_LENGTH];

        loop {
            match self.reader.read(&mut header[..1]) {
                Ok(0) => {
                    self.finished = true;
                    return Ok(None);
                }
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.finished = true;
                    return Err(StorageError::Io(e));
                }
            }
        }

        match self.reader.read_exact(&mut header[1..]) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.finished = true;
                return Err(StorageError::Codec(CodecError::IncompleteHeader));
            }
            Err(e) => {
                self.finished = true;
                return Err(StorageError::Io(e));
            }
        }

        let record_len = match Record::encoded_record_len(&header, self.limits) {
            Ok(len) => len,
            Err(e) => {
                self.finished = true;
                return Err(StorageError::Codec(e));
            }
        };

        let mut buffer = vec![0u8; record_len];
        buffer[..HEADER_LENGTH].copy_from_slice(&header);

        match self.reader.read_exact(&mut buffer[HEADER_LENGTH..]) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.finished = true;
                return Err(StorageError::Codec(CodecError::IncompleteBody));
            }
            Err(e) => {
                self.finished = true;
                return Err(StorageError::Io(e));
            }
        }

        let record = match Record::decode(&buffer, self.limits) {
            Ok((record, _)) => record,
            Err(e) => {
                self.finished = true;
                return Err(StorageError::Codec(e));
            }
        };

        Ok(Some(record))
    }
}

impl Iterator for LogScanner<'_> {
    type Item = Result<Record, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Debug)]
pub struct Log {
    file: File,
    limits: RecordLimits,
    path: std::path::PathBuf,
}

impl Log {
    pub fn open(path: &Path, limits: RecordLimits) -> Result<Self, StorageError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Log {
            file,
            limits,
            path: path.to_path_buf(),
        })
    }

    pub fn append(&mut self, record: &Record) -> Result<(), StorageError> {
        let bytes = record.encode(&self.limits)?;
        self.file.write_all(&bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        Ok(LogScanner {
            reader,
            limits: &self.limits,
            finished: false,
        })
    }
}
