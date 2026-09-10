use crate::error::CodecError;
use crate::storage::record::HEADER_LENGTH;
use crate::storage::record::Record;
use crate::{error::StorageError, storage::record::RecordLimits};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, Read, Write};
use std::path::Path;

trait AppendIo: Write {
    fn sync_data(&self) -> std::io::Result<()>;
}

impl AppendIo for File {
    fn sync_data(&self) -> std::io::Result<()> {
        File::sync_data(self)
    }
}

struct Reader<'a> {
    file: &'a File,
    position: u64,
}

impl<'a> Reader<'a> {
    fn new(file: &'a File) -> Self {
        Reader { file, position: 0 }
    }
}

impl<'a> std::io::Read for Reader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;

        #[cfg(unix)]
        match self.file.read_at(buf, self.position) {
            Ok(bytes_read) => {
                self.position += bytes_read as u64;
                Ok(bytes_read)
            }
            Err(e) => Err(e),
        }

        #[cfg(windows)]
        use std::os::windows::fs::FileExt;

        #[cfg(windows)]
        match self.file.seek_read(buf, self.position) {
            Ok(bytes_read) => {
                self.position += bytes_read as u64;
                Ok(bytes_read)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct LogScanner<'a> {
    reader: BufReader<Reader<'a>>,
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
    append_failed: bool,
}

impl Log {
    fn write_record(writer: &mut impl AppendIo, append_failed: &mut bool, bytes: &[u8]) -> Result<(), StorageError> {
        if *append_failed {
            return Err(StorageError::AppendDisabled);
        }

        match writer.write_all(bytes) {
            Ok(_) => {}
            Err(e) => {
                *append_failed = true;
                return Err(StorageError::Io(e));
            }
        }
        match writer.flush() {
            Ok(_) => {}
            Err(e) => {
                *append_failed = true;
                return Err(StorageError::Io(e));
            }
        }
        match writer.sync_data() {
            Ok(_) => {}
            Err(e) => {
                *append_failed = true;
                return Err(StorageError::Io(e));
            }
        }

        Ok(())
    }

    pub fn open(path: &Path, limits: RecordLimits) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Log {
            file,
            limits,
            append_failed: false,
        })
    }

    pub fn append(&mut self, record: &Record) -> Result<(), StorageError> {
        if self.append_failed {
            return Err(StorageError::AppendDisabled);
        }
        
        let bytes = record.encode(&self.limits)?;
        Self::write_record(&mut self.file, &mut self.append_failed, &bytes)
    }

    pub fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        let reader = Reader::new(&self.file);
        Ok(LogScanner {
            reader: BufReader::new(reader),
            limits: &self.limits,
            finished: false,
        })
    }
}
