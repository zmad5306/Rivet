use std::io::Write;
use std::path::{Path, PathBuf};

use super::{ConsumerGroupName, OFFSET_STORE_DIR};
use crate::broker::topic::TopicName;
use crate::error::OffsetStoreError;

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

type PublishOperation = fn(&Path, &Path) -> std::io::Result<()>;

#[derive(Debug)]
pub(crate) struct OffsetStore {
    root: PathBuf,
    publish_operation: PublishOperation,
}

fn publish(temp_path: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(&temp_path, &path)
}

#[cfg(test)]
fn publish_with_failure(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::other("injected pre-publication failure"))
}

impl OffsetStore {
    pub(crate) fn new(data_root: &Path) -> Self {
        let root = data_root.join(OFFSET_STORE_DIR);
        Self {
            root,
            publish_operation: publish,
        }
    }

    #[cfg(test)]
    fn new_with_publish_operation(data_root: &Path, publish_operation: PublishOperation) -> Self {
        let root = data_root.join(OFFSET_STORE_DIR);
        Self {
            root,
            publish_operation: publish_operation,
        }
    }

    pub(crate) fn offset_path(
        &self,
        group: &ConsumerGroupName,
        topic: &TopicName,
        partition: u32,
    ) -> Result<PathBuf, OffsetStoreError> {
        if partition != 0 {
            return Err(OffsetStoreError::UnsupportedPartitionId {
                requested: partition,
            });
        }

        Ok(self
            .root
            .join(group.as_str())
            .join(topic.as_str())
            .join(format!("{}.offset", partition)))
    }

    /// Reads committed state only from the final `<partition>.offset` path.
    ///
    /// Sibling temporary artifacts are uncommitted state and are ignored. Lookup
    /// is read-only: it neither removes temporary artifacts nor creates files or
    /// directories. Cleanup, when safe and necessary, belongs to the operation
    /// that owns the temporary artifact.
    pub(crate) fn get_committed_offset(
        &self,
        group: &ConsumerGroupName,
        topic: &TopicName,
        partition: u32,
    ) -> Result<Option<u64>, OffsetStoreError> {
        let path = self.offset_path(group, topic, partition)?;

        let offset_file_content = match std::fs::read(&path) {
            Ok(content) => content,
            Err(source) => {
                if source.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(OffsetStoreError::Io { source, path });
                }
            }
        };

        let offset = match std::str::from_utf8(&offset_file_content) {
            Ok(text) => match text.parse::<u64>() {
                Ok(offset) => {
                    if offset.to_string() != text {
                        return Err(OffsetStoreError::MalformedOffset { path });
                    }
                    offset
                }
                Err(_) => {
                    return Err(OffsetStoreError::MalformedOffset { path });
                }
            },
            Err(_) => {
                return Err(OffsetStoreError::MalformedOffset { path });
            }
        };

        Ok(Some(offset))
    }

    fn ensure_real_directory(path: &Path) -> Result<(), OffsetStoreError> {
        match std::fs::create_dir(path) {
            Ok(_) => Ok(()),
            Err(source) => {
                if source.kind() == std::io::ErrorKind::AlreadyExists {
                    match std::fs::symlink_metadata(path) {
                        Ok(metadata) => {
                            let file_type = metadata.file_type();
                            if !file_type.is_dir() || file_type.is_symlink() {
                                return Err(OffsetStoreError::UnsafePath {
                                    path: path.to_path_buf(),
                                });
                            }
                            Ok(())
                        }
                        Err(source) => {
                            return Err(OffsetStoreError::Io {
                                source,
                                path: path.to_path_buf(),
                            });
                        }
                    }
                } else {
                    Err(OffsetStoreError::Io {
                        source,
                        path: path.to_path_buf(),
                    })
                }
            }
        }
    }

    fn sync_directory(path: &Path) -> Result<(), OffsetStoreError> {
        #[cfg(unix)]
        {
            match std::fs::File::open(path) {
                Ok(file) => match file.sync_all() {
                    Ok(_) => {}
                    Err(source) => {
                        return Err(OffsetStoreError::Io {
                            source,
                            path: path.to_path_buf(),
                        });
                    }
                },
                Err(source) => {
                    return Err(OffsetStoreError::Io {
                        source,
                        path: path.to_path_buf(),
                    });
                }
            }
        }

        #[cfg(windows)]
        {
            // Stable Rust does not expose the directory-handle semantics needed to
            // durably sync this renamed directory entry on Windows. The offset file
            // contents are synced before publication, but power-loss durability of
            // the rename itself is not guaranteed.
            let _ = path;
        }

        Ok(())
    }

    pub(crate) fn commit_offset(
        &self,
        group: &ConsumerGroupName,
        topic: &TopicName,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let path = self.offset_path(group, topic, partition)?;

        Self::ensure_real_directory(&self.root)?;

        let group_dir = self.root.join(group.as_str());
        Self::ensure_real_directory(&group_dir)?;

        let topic_dir = group_dir.join(topic.as_str());
        Self::ensure_real_directory(&topic_dir)?;

        debug_assert!(path.parent() == Some(topic_dir.as_path()));

        match self.get_committed_offset(group, topic, partition)? {
            Some(current) if current > next_offset => {
                return Err(OffsetStoreError::Rewind {
                    current,
                    requested: next_offset,
                });
            }
            Some(current) if current == next_offset => {
                return Ok(());
            }
            Some(_) | None => {
                // Do nothing; execution continues to temporary-file creation.
            }
        }

        let process_id = std::process::id();
        let (temp_path, mut temp_file) = loop {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let tmp_path = topic_dir.join(format!("0.offset.{}.{}.tmp", process_id, counter));

            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
            {
                Ok(file) => {
                    break (tmp_path, file);
                }
                Err(source) => {
                    if source.kind() == std::io::ErrorKind::AlreadyExists {
                        continue;
                    }
                    return Err(OffsetStoreError::Io {
                        source,
                        path: tmp_path.clone(),
                    });
                }
            }
        };

        let next_offset_canonical = next_offset.to_string();
        let write_result = (|| -> std::io::Result<()> {
            temp_file.write_all(next_offset_canonical.as_bytes())?;
            temp_file.flush()?;
            temp_file.sync_data()?;
            Ok(())
        })();

        drop(temp_file);

        if let Err(source) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(OffsetStoreError::Io {
                source,
                path: temp_path,
            });
        } else {
            let rename_result = (self.publish_operation)(&temp_path, &path);
            if let Err(source) = rename_result {
                let _ = std::fs::remove_file(&temp_path);
                return Err(OffsetStoreError::Io { source, path });
            }
        }

        Self::sync_directory(&topic_dir)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use crate::{
        broker::topic::TopicName,
        consumer::{
            ConsumerGroupName, OFFSET_STORE_DIR,
            offset_store::{OffsetStore, publish_with_failure},
        },
        error::OffsetStoreError,
    };

    #[test]
    fn offset_path_maps_validated_identity_components_under_reserved_root() {
        let data_root = Path::new("/data");
        let offset_store = OffsetStore::new(data_root);
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let actual_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");

        assert_eq!(
            actual_path,
            Path::new("/data/__consumer_offsets/fraud-detector/orders/0.offset"),
            "offset path should match the expected structure"
        );
    }

    #[test]
    fn offset_path_rejects_unsupported_partition_id() {
        let data_root = Path::new("/data");
        let offset_store = OffsetStore::new(data_root);
        let group = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let topic =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let error = offset_store
            .offset_path(&group, &topic, 1)
            .expect_err("should fail for unsupported partition id");

        assert!(matches!(
            &error,
            OffsetStoreError::UnsupportedPartitionId { requested: 1 }
        ));
        assert_eq!(
            error.to_string(),
            "unsupported partition id: 1, only partition id 0 is supported",
            "unexpected error message"
        );
        assert!(
            error.source().is_none(),
            "unsupported partition id error should have no source"
        );
    }

    #[test]
    fn missing_committed_offset_returns_none_without_creating_storage() {
        let root = tempfile::tempdir().expect("failed to create temporary data root");
        let data_root = root.path().join("data");
        std::fs::create_dir_all(&data_root).expect("failed to create data root");

        let offset_store = OffsetStore::new(&data_root);
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let committed_offset = offset_store
            .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");

        assert!(
            committed_offset.is_none(),
            "missing committed offset should return None"
        );
        assert!(
            !data_root.join(OFFSET_STORE_DIR).exists(),
            "offset store directory should not be created after missing committed offset lookup"
        );
    }

    #[test]
    fn committed_offset_lookup_parses_zero_and_u64_max() {
        let root = tempfile::tempdir().expect("failed to create temporary data root");
        let data_root = root.path().join("data");
        let offset_store = OffsetStore::new(&data_root);
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");
        let parent = final_offset_path
            .parent()
            .expect("final offset path has no parent directory");

        std::fs::create_dir_all(parent)
            .expect("failed to create parent directories for final offset file");
        std::fs::write(&final_offset_path, b"0").expect("failed to write initial offset value");

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("should succeed for valid partition id 0"),
            Some(0),
            "committed offset for partition 0 should be 0"
        );

        std::fs::write(&final_offset_path, u64::MAX.to_string().as_bytes())
            .expect("failed to write maximum offset value");
        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("should succeed for valid partition id 0"),
            Some(u64::MAX),
            "committed offset for partition 0 should be u64::MAX"
        );
    }

    #[test]
    fn committed_offset_lookup_rejects_noncanonical_and_malformed_contents() {
        let root = tempfile::tempdir().expect("failed to create temporary root");
        let data_root = root.path().join("data");
        let offset_store = OffsetStore::new(&data_root);
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");
        let parent = final_offset_path
            .parent()
            .expect("final offset path has no parent directory");

        std::fs::create_dir_all(parent)
            .expect("failed to create parent directories for final offset file");

        let fixtures: &[&[u8]] = &[
            b"",                     // empty bytes
            b"abc",                  // non-decimal bytes
            b"18446744073709551616", // overflow bytes (u64::MAX + 1)
            b"1\n",                  // trailing-junk bytes
            b"01",                   // noncanonical leading-zero bytes
        ];

        for fixture in fixtures.iter() {
            std::fs::write(&final_offset_path, fixture)
                .expect("failed to write fixture to final offset file");
            let result = offset_store.get_committed_offset(
                &fraud_detector_group_name,
                &orders_topic_name,
                0,
            );
            assert!(
                matches!(result, Err(OffsetStoreError::MalformedOffset { path }) if path == final_offset_path)
            );
        }
    }

    #[test]
    fn committed_offset_lookup_ignores_leftover_temporary_file() {
        let root = tempfile::tempdir().expect("failed to create temporary root");
        let data_root = root.path().join("data");
        let offset_store = OffsetStore::new(&data_root);
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");
        let parent = final_offset_path
            .parent()
            .expect("final offset path has no parent directory");

        std::fs::create_dir_all(parent)
            .expect("failed to create parent directories for final offset file");

        let temp_offset_path = final_offset_path.with_extension("offset.tmp");
        std::fs::write(&temp_offset_path, b"43").expect("failed to write temporary offset file");

        let result =
            offset_store.get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0);
        assert!(matches!(result, Ok(None)));

        let temp_contents =
            std::fs::read(&temp_offset_path).expect("failed to read temporary offset file");
        assert_eq!(temp_contents, b"43");

        std::fs::write(&final_offset_path, b"42").expect("failed to write final offset file");

        let result =
            offset_store.get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0);
        assert!(matches!(result, Ok(Some(42))));

        let temp_contents =
            std::fs::read(&temp_offset_path).expect("failed to read temporary offset file");
        assert_eq!(temp_contents, b"43");

        let final_contents =
            std::fs::read(&final_offset_path).expect("failed to read final offset file");
        assert_eq!(final_contents, b"42");
    }

    #[test]
    fn committed_offset_lookup_rejects_malformed_final_file_even_with_valid_temporary_file() {
        let root = tempfile::tempdir().expect("failed to create temporary root");
        let offset_store = OffsetStore::new(root.path());
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");
        let temp_offset_path = final_offset_path.with_extension("tmp");

        std::fs::create_dir_all(
            final_offset_path
                .parent()
                .expect("failed to get parent directory for final offset file"),
        )
        .expect("failed to create parent directories for final offset file");
        std::fs::write(&final_offset_path, b"not-a-number")
            .expect("failed to write malformed final offset file");
        std::fs::write(&temp_offset_path, b"43").expect("failed to write temporary offset file");

        let error = offset_store
            .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect_err("expected malformed offset error");

        assert!(
            matches!(error, OffsetStoreError::MalformedOffset { path } if path == final_offset_path),
            "expected malformed offset error for the final offset file"
        );

        let final_contents =
            std::fs::read(&final_offset_path).expect("failed to read final offset file");

        assert_eq!(
            final_contents, b"not-a-number",
            "final offset file should contain the malformed offset"
        );

        let temp_contents =
            std::fs::read(&temp_offset_path).expect("failed to read temporary offset file");

        assert_eq!(
            temp_contents, b"43",
            "temporary offset file should contain the valid offset"
        );
    }

    #[test]
    fn commit_offset_persists_first_forward_and_idempotent_values() {
        let root = tempfile::tempdir().expect("failed to create temporary root");
        let offset_store = OffsetStore::new(root.path());
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");

        offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 42)
            .expect("failed to commit offset");

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset")
                .expect("no committed offset found"),
            42
        );
        assert_eq!(
            std::fs::read(&final_offset_path).expect("failed to read final offset file"),
            b"42".to_vec(),
            "final offset file should contain the committed offset 42"
        );

        offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 42)
            .expect("failed to commit offset");

        assert_eq!(
            std::fs::read(&final_offset_path).expect("failed to read final offset file"),
            b"42".to_vec(),
            "final offset file should still contain the committed offset 42"
        );
        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset")
                .expect("no committed offset found"),
            42
        );

        let files = std::fs::read_dir(
            final_offset_path
                .parent()
                .expect("failed to get parent directory"),
        )
        .expect("failed to list files in offset path");
        let mut asserted = false;
        for file in files {
            let file = file.expect("failed to read file in offset path");
            let file_name = file.file_name();
            let file_name = file_name
                .to_str()
                .expect("failed to convert file name to string");
            assert!(
                !file_name.ends_with(".tmp"),
                "temporary offset file should not exist without a sibling `.tmp` file"
            );
            asserted = true;
        }
        assert!(asserted, "no files were found in the offset path");

        offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 43)
            .expect("failed to commit offset");

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset")
                .expect("no committed offset found"),
            43
        );
        assert_eq!(
            std::fs::read(&final_offset_path).expect("failed to read final offset file"),
            b"43".to_vec(),
            "final offset file should now contain the committed offset 43"
        );

        let files = std::fs::read_dir(
            final_offset_path
                .parent()
                .expect("failed to get parent directory"),
        )
        .expect("failed to list files in offset path");
        let mut asserted = false;
        for file in files {
            let file = file.expect("failed to read file in offset path");
            let file_name = file.file_name();
            let file_name = file_name
                .to_str()
                .expect("failed to convert file name to string");
            assert!(
                !file_name.ends_with(".tmp"),
                "temporary offset file should not exist without a sibling `.tmp` file"
            );
            asserted = true;
        }
        assert!(asserted, "no files were found in the offset path");
    }

    #[test]
    fn commit_offset_rejects_rewind_and_preserves_committed_value() {
        let root = tempfile::tempdir().expect("failed to create temporary root");
        let offset_store = OffsetStore::new(root.path());
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should succeed for valid group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("should succeed for valid topic name");
        let final_offset_path = offset_store
            .offset_path(&fraud_detector_group_name, &orders_topic_name, 0)
            .expect("should succeed for valid partition id 0");

        offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 43)
            .expect("failed to commit offset");

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset")
                .expect("no committed offset found"),
            43
        );
        assert_eq!(
            std::fs::read(&final_offset_path).expect("failed to read final offset file"),
            b"43".to_vec(),
            "final offset file should contain the committed offset 43"
        );

        let error = offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 42)
            .expect_err("expected rewind error");

        assert!(matches!(
            error,
            OffsetStoreError::Rewind {
                current: 43,
                requested: 42
            }
        ));

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset")
                .expect("no committed offset found"),
            43
        );
        assert_eq!(
            std::fs::read(&final_offset_path).expect("failed to read final offset file"),
            b"43".to_vec(),
            "final offset file should still contain the committed offset 43"
        );

        let mut asserted = false;
        for entry in std::fs::read_dir(
            &final_offset_path
                .parent()
                .expect("failed to get parent directory"),
        )
        .expect("failed to read offset path")
        {
            let entry = entry.expect("failed to read directory entry");
            let file = entry.path();
            let file_name = file.file_name().expect("failed to get file name");
            let file_name = file_name
                .to_str()
                .expect("failed to convert file name to string");
            assert!(
                !file_name.ends_with(".tmp"),
                "temporary offset file should not exist without a sibling `.tmp` file"
            );
            asserted = true;
        }
        assert!(asserted, "no files were found in the offset path");
    }

    #[test]
    fn committed_offsets_survive_reopen_with_group_and_topic_isolation() {
        let root = tempfile::tempdir().expect("failed to create temporary data root");
        let offset_store = OffsetStore::new(root.path());
        let fraud_detector_group_name = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("failed to create fraud detector group name");
        let analytics_group_name = ConsumerGroupName::new("analytics".to_string())
            .expect("failed to create analytics group name");
        let orders_topic_name =
            TopicName::new("orders".to_string()).expect("failed to create orders topic name");
        let payments_topic_name =
            TopicName::new("payments".to_string()).expect("failed to create payments topic name");

        offset_store
            .commit_offset(&fraud_detector_group_name, &orders_topic_name, 0, 42)
            .expect("failed to commit offset");
        offset_store
            .commit_offset(&analytics_group_name, &orders_topic_name, 0, 43)
            .expect("failed to commit offset");
        offset_store
            .commit_offset(&fraud_detector_group_name, &payments_topic_name, 0, 44)
            .expect("failed to commit offset");
        offset_store
            .commit_offset(&analytics_group_name, &payments_topic_name, 0, 45)
            .expect("failed to commit offset");

        drop(offset_store);

        let offset_store = OffsetStore::new(root.path());

        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset"),
            Some(42),
            "committed offset for fraud-detector/orders should be 42"
        );
        assert_eq!(
            offset_store
                .get_committed_offset(&analytics_group_name, &orders_topic_name, 0)
                .expect("failed to get committed offset"),
            Some(43),
            "committed offset for analytics/orders should be 43"
        );
        assert_eq!(
            offset_store
                .get_committed_offset(&fraud_detector_group_name, &payments_topic_name, 0)
                .expect("failed to get committed offset"),
            Some(44),
            "committed offset for fraud-detector/payments should be 44"
        );
        assert_eq!(
            offset_store
                .get_committed_offset(&analytics_group_name, &payments_topic_name, 0)
                .expect("failed to get committed offset"),
            Some(45),
            "committed offset for analytics/payments should be 45"
        );
        assert_eq!(
            offset_store
                .get_committed_offset(
                    &fraud_detector_group_name,
                    &TopicName::new("non-existent".to_string())
                        .expect("failed to create non-existent topic name"),
                    0
                )
                .expect("failed to get committed offset"),
            None,
            "committed offset for non-existent topic should be None"
        );
    }

    #[test]
    fn commit_offset_rejects_file_at_reserved_directory_path() {
        let sentinal_data = b"sentinal-data";
        let root = tempfile::tempdir().expect("failed to create data root");
        let sentinel_path = root.path().join(OFFSET_STORE_DIR);

        std::fs::write(&sentinel_path, sentinal_data).expect("failed to write sentinal file");

        let store = OffsetStore::new(root.path());
        let group = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("failed to create consumer group name");
        let topic = TopicName::new("orders".to_string()).expect("failed to create topic name");

        let error = store
            .commit_offset(&group, &topic, 0, 42)
            .expect_err("should fail to commit");

        assert!(
            matches!(error, OffsetStoreError::UnsafePath { path } if path == sentinel_path),
            "error must match"
        );
        assert_eq!(
            std::fs::read(&sentinel_path).expect("failed to read sentinel file"),
            sentinal_data,
            "sentinel in file should not have chnaged"
        );

        let group_path = sentinel_path.join(group.as_str());
        assert!(
            !group_path.exists(),
            "consumer group data should not have been created"
        );
    }

    #[test]
    fn commit_offset_rejects_file_at_group_directory_path() {
        let data_root = tempfile::tempdir().expect("failed to create data root");
        let analytics_consumer_group = ConsumerGroupName::new("analytics".to_string())
            .expect("failed to create analytics consumer group name");
        let fraud_detector_consumer_group = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("failed to create fraud detector consumer group name");
        let orders_topic =
            TopicName::new("orders".to_string()).expect("failed to create orders topic name");

        let offset_store = OffsetStore::new(data_root.path());

        offset_store
            .commit_offset(&analytics_consumer_group, &orders_topic, 0, 57)
            .expect("commit offset failed");

        let sentinal_data = b"sentinal-data";
        let sentinel_path = data_root
            .path()
            .join(OFFSET_STORE_DIR)
            .join(fraud_detector_consumer_group.as_str());

        std::fs::write(&sentinel_path, sentinal_data).expect("failed to write sentinal file");

        let error = offset_store
            .commit_offset(&fraud_detector_consumer_group, &orders_topic, 0, 32)
            .expect_err("write should faile when offset dir is a file");

        assert!(
            matches!(error, OffsetStoreError::UnsafePath { path } if path == sentinel_path),
            "found unexpected error shape"
        );
        assert_eq!(
            std::fs::read(&sentinel_path).expect("failed to read sentinel file"),
            sentinal_data,
            "sentinel in file should not have chnaged"
        );

        let topic_path = sentinel_path.join(orders_topic.as_str());
        assert!(!topic_path.exists(), "topic data should not exist");

        let analytics_committed_offset = offset_store
            .get_committed_offset(&analytics_consumer_group, &orders_topic, 0)
            .expect("failed to get committed offset")
            .expect("comitted offset not populated");
        assert_eq!(
            analytics_committed_offset, 57,
            "commited offset should not have changed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn commit_offset_rejects_symlink_at_group_directory_path() {
        let data_root = tempfile::tempdir().expect("failed to create data root");
        let symlink_target = data_root.path().join("target");

        std::fs::create_dir(&symlink_target).expect("failed to create symlink target");

        let fraud_detector_consumer_group = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("failed to create fraud detector consumer group name");
        let orders_topic =
            TopicName::new("orders".to_string()).expect("failed to create orders topic name");

        let offset_store = OffsetStore::new(data_root.path());

        let offset_store_dir = data_root.path().join(OFFSET_STORE_DIR);
        let fraud_detection_dir = offset_store_dir
            .as_path()
            .join(fraud_detector_consumer_group.as_str());

        std::fs::create_dir(offset_store_dir).expect("failed to create offset store dir");
        std::os::unix::fs::symlink(&symlink_target, &fraud_detection_dir)
            .expect("failed to create symlink");

        let error = offset_store
            .commit_offset(&fraud_detector_consumer_group, &orders_topic, 0, 32)
            .expect_err("should have failed to commit");
        let orders_path = symlink_target.join(orders_topic.as_str());
        let final_offset_path = orders_path.join("0.offset");
        let metadata =
            std::fs::symlink_metadata(&fraud_detection_dir).expect("failed to get metadata");
        let mut target_entries =
            std::fs::read_dir(&symlink_target).expect("failed to read symlink targent");

        assert!(
            matches!(error, OffsetStoreError::UnsafePath { path } if path == fraud_detection_dir),
            "unexpected error shape"
        );
        assert!(symlink_target.exists(), "missing symlink target");
        assert!(fraud_detection_dir.exists(), "missing symlink");
        assert!(
            metadata.file_type().is_symlink(),
            "fraud detection consumer group dir should have been a symlink"
        );
        assert!(
            !orders_path.exists(),
            "should not have followed symlink and created offset dir"
        );
        assert!(
            !final_offset_path.exists(),
            "should not have followed symlink and created an offset file"
        );
        assert!(
            target_entries.next().is_none(),
            "symlink target should contain no created artifacts"
        );
    }

    #[test]
    fn commit_offset_prepublication_failure_preserves_prior_value() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let fraud_detector_consumer_group = ConsumerGroupName::new("fraud-detector".to_string())
            .expect("should accept valid fraud-detector consumer group name");
        let orders_topic =
            TopicName::new("orders".to_string()).expect("should accept valid orders topic name");

        let offset_store = OffsetStore::new(data_root.path());

        let final_offset_path = offset_store
            .offset_path(&fraud_detector_consumer_group, &orders_topic, 0)
            .expect("should derive final offset path for supported partition 0");

        offset_store
            .commit_offset(&fraud_detector_consumer_group, &orders_topic, 0, 42)
            .expect("initial commit of offset 42 should succeed");

        let topic_directory = final_offset_path
            .parent()
            .expect("generated final offset path should have a topic directory");
        let unrelated_temp_path = topic_directory.join("unrelated.tmp");
        let do_not_touch = b"do-not-touch";

        std::fs::write(&unrelated_temp_path, do_not_touch)
            .expect("failed to create unrelated sibling temporary artifact");

        let offset_store =
            OffsetStore::new_with_publish_operation(data_root.path(), publish_with_failure);
        let error = offset_store
            .commit_offset(&fraud_detector_consumer_group, &orders_topic, 0, 43)
            .expect_err("forward commit should fail at injected publication boundary");
        let file_data = std::fs::read(&final_offset_path)
            .expect("failed to read prior committed offset after publication failure");
        let file_entries = std::fs::read_dir(
            final_offset_path
                .parent()
                .expect("generated final offset path should have a topic directory"),
        )
        .expect("failed to list topic directory after publication failure");
        let unrelated_file_data = std::fs::read(unrelated_temp_path)
            .expect("failed to read unrelated sibling temporary artifact");
        let offset = offset_store
            .get_committed_offset(&fraud_detector_consumer_group, &orders_topic, 0)
            .expect("committed-offset lookup should succeed after publication failure")
            .expect("prior committed offset should remain present after publication failure");

        assert_eq!(
            offset, 42,
            "failed publication should leave the prior committed offset authoritative"
        );
        assert!(
            matches!(error, OffsetStoreError::Io { path, source } if path == final_offset_path && source.kind() == std::io::ErrorKind::Other && source.to_string() == "injected pre-publication failure"),
            "failed commit should return the injected I/O error with final offset path context"
        );
        assert_eq!(
            file_data, b"42",
            "failed publication should preserve the prior committed bytes"
        );

        let mut entry_names = file_entries
            .map(|entry| {
                entry
                    .expect("failed to inspect an entry in the topic directory")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();

        entry_names.sort();

        assert_eq!(
            entry_names,
            vec!["0.offset".to_string(), "unrelated.tmp".to_string()],
            "failed commit should remove its owned temporary file and preserve unrelated entries"
        );

        assert_eq!(
            do_not_touch.to_vec(),
            unrelated_file_data,
            "failed-commit cleanup should not modify unrelated temporary-file contents"
        );
    }
}
