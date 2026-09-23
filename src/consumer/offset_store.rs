use std::path::{Path, PathBuf};

use super::{ConsumerGroupName, OFFSET_STORE_DIR};
use crate::broker::topic::TopicName;
use crate::error::OffsetStoreError;

#[derive(Debug)]
pub(crate) struct OffsetStore {
    root: PathBuf,
}

impl OffsetStore {
    pub(crate) fn new(data_root: &Path) -> Self {
        let root = data_root.join(OFFSET_STORE_DIR);
        Self { root }
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
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use crate::{
        broker::topic::TopicName,
        consumer::{ConsumerGroupName, OFFSET_STORE_DIR, offset_store::OffsetStore},
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
        // TODO: Create a temporary data root, an `OffsetStore`, validated group
        // `fraud-detector`, and validated topic `orders`; derive partition 0's final
        // offset path through `offset_path` and create only its parent directories.
        // TODO: Create a sibling temporary file whose name is the final filename plus
        // `.tmp`, containing canonical bytes `43`; with no final offset file present,
        // verify lookup returns `None` and leaves the temporary file unchanged.
        // TODO: Write canonical bytes `42` to the final offset file, look up the same
        // identity again, and verify the final file remains authoritative as `Some(42)`
        // while the sibling temporary file still exists unchanged.
        todo!()
    }
}
