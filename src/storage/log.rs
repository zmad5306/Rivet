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
    fn write_record(
        writer: &mut impl AppendIo,
        append_failed: &mut bool,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
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

    fn recover(&mut self) -> Result<u64, StorageError> {
        let mut next_offset = 0;
        let mut bytes_read: usize = 0;
        let scanner = self.scan()?;

        for result in scanner {
            match result {
                Ok(record) => {
                    if record.offset() != next_offset {
                        return Err(StorageError::UnexpectedOffset {
                            expected: next_offset,
                            actual: record.offset(),
                        });
                    }
                    next_offset = record
                        .offset()
                        .checked_add(1)
                        .ok_or(StorageError::OffsetOverflow)?;
                    let encoded_len = match record.encoded_len() {
                        Ok(len) => len,
                        Err(err) => return Err(StorageError::Codec(err)),
                    };
                    bytes_read = bytes_read
                        .checked_add(encoded_len)
                        .ok_or(StorageError::Codec(CodecError::LengthOverflow))?;
                }
                Err(StorageError::Codec(
                    CodecError::IncompleteHeader | CodecError::IncompleteBody,
                )) => {
                    let bytes_read_converted = match u64::try_from(bytes_read) {
                        Ok(val) => val,
                        Err(_) => return Err(StorageError::Codec(CodecError::LengthOverflow)),
                    };
                    self.file.set_len(bytes_read_converted)?;
                    self.file.sync_data()?;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(next_offset)
    }

    pub fn open(path: &Path, limits: RecordLimits) -> Result<(Self, u64), StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        let mut log = Log {
            file,
            limits,
            append_failed: false,
        };

        let next_offset = log.recover()?;

        Ok((log, next_offset))
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

#[cfg(test)]
mod tests {

    use super::*;

    struct FailAfterBytes {
        written: Vec<u8>,
        remaining: usize,
        fail_flush: bool,
        fail_sync: bool,
    }

    impl Write for FailAfterBytes {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }

            if self.remaining == 0 {
                return Err(std::io::Error::other("injected write failure"));
            }

            let to_write = std::cmp::min(buf.len(), self.remaining);
            self.written.extend_from_slice(&buf[..to_write]);
            self.remaining -= to_write;
            Ok(to_write)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.fail_flush {
                return Err(std::io::Error::other("injected flush failure"));
            }
            Ok(())
        }
    }

    impl AppendIo for FailAfterBytes {
        fn sync_data(&self) -> std::io::Result<()> {
            if self.fail_sync {
                return Err(std::io::Error::other("injected sync failure"));
            }
            Ok(())
        }
    }

    #[test]
    fn partial_write_failure_rejects_later_appends() {
        let record = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record_bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");
        let mut writer = FailAfterBytes {
            written: Vec::new(),
            remaining: 5,
            fail_flush: false,
            fail_sync: false,
        };
        let mut append_failed = false;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected a write failure");

        match error {
            StorageError::Io(io_error) => {
                assert_eq!(io_error.kind(), std::io::ErrorKind::Other);
                assert_eq!(io_error.to_string(), "injected write failure");
            }
            other => panic!("expected an I/O error, got {other:?}"),
        }

        assert!(
            append_failed,
            "append_failed should be set to true after a write failure"
        );
        assert_eq!(writer.written, &record_bytes[..5]);

        writer.remaining = record_bytes.len() + 1;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected appending to be disabled");

        assert!(matches!(error, StorageError::AppendDisabled));
        assert_eq!(writer.written, &record_bytes[..5]);
    }

    #[test]
    fn flush_failure_rejects_later_appends() {
        let record = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record_bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");
        let mut writer = FailAfterBytes {
            written: Vec::new(),
            remaining: record_bytes.len() + 1,
            fail_flush: true,
            fail_sync: false,
        };
        let mut append_failed = false;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected a write failure");

        match error {
            StorageError::Io(io_error) => {
                assert_eq!(io_error.kind(), std::io::ErrorKind::Other);
                assert_eq!(io_error.to_string(), "injected flush failure");
            }
            other => panic!("expected an I/O error, got {other:?}"),
        }

        assert!(
            append_failed,
            "append_failed should be set to true after a flush failure"
        );
        assert_eq!(writer.written, record_bytes);
        writer.fail_flush = false;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected appending to be disabled after a flush failure");

        assert!(matches!(error, StorageError::AppendDisabled));
    }

    #[test]
    fn sync_failure_rejects_later_appends() {
        let record = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record_bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");
        let mut writer = FailAfterBytes {
            written: Vec::new(),
            remaining: record_bytes.len() + 1,
            fail_flush: false,
            fail_sync: true,
        };
        let mut append_failed = false;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected a write failure");

        match error {
            StorageError::Io(io_error) => {
                assert_eq!(io_error.kind(), std::io::ErrorKind::Other);
                assert_eq!(io_error.to_string(), "injected sync failure");
            }
            other => panic!("expected an I/O error, got {other:?}"),
        }

        assert!(
            append_failed,
            "append_failed should be set to true after a write failure"
        );
        assert_eq!(writer.written, record_bytes);

        writer.remaining = record_bytes.len() + 1;

        let error = Log::write_record(&mut writer, &mut append_failed, &record_bytes)
            .expect_err("expected appending to be disabled");

        assert!(matches!(error, StorageError::AppendDisabled));
        assert_eq!(writer.written, record_bytes);
    }

    #[test]
    fn scanning_after_append_failure_preserves_valid_prefix() {
        let record1 = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record2 = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record1_bytes = record1
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");
        let record2_bytes = record2
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");
        let mut writer = FailAfterBytes {
            written: Vec::new(),
            remaining: record1_bytes.len() + 5,
            fail_flush: false,
            fail_sync: false,
        };
        let mut append_failed = false;

        Log::write_record(&mut writer, &mut append_failed, &record1_bytes)
            .expect("first record should succeed");

        assert!(
            !append_failed,
            "append_failed should be false after a successful write"
        );

        let error = Log::write_record(&mut writer, &mut append_failed, &record2_bytes)
            .expect_err("expected a partial write failure");

        assert!(
            append_failed,
            "append_failed should be true after a partial write failure"
        );
        assert!(
            matches!(error, StorageError::Io(_)),
            "expected an I/O error for the partial write failure"
        );
        assert_eq!(
            writer.written,
            [record1_bytes.as_slice(), &record2_bytes[..5]].concat()
        );

        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let path = dir.path().join("failed-append.log");

        let (mut log, _) =
            Log::open(&path, RecordLimits::default()).expect("opening the log should succeed");

        std::fs::write(&path, &writer.written)
            .expect("writing the simulated failed-append contents should succeed");

        log.append_failed = append_failed;

        let mut scanner = log.scan().expect("scanning the log should succeed");
        let result1 = scanner
            .next()
            .expect("expected the first record to be present");

        assert!(
            result1.is_ok(),
            "expected the first record to be successfully read"
        );
        assert_eq!(result1.unwrap(), record1);

        let result2 = scanner
            .next()
            .expect("expected the second record to be present");
        assert!(
            result2.is_err(),
            "expected the second record to fail due to incomplete write"
        );

        assert!(matches!(
            result2,
            Err(StorageError::Codec(CodecError::IncompleteHeader))
        ));

        let result3 = scanner.next();
        assert!(
            result3.is_none(),
            "expected no more records after the incomplete second record"
        );
    }
}
