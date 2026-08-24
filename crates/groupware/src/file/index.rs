/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{ArchivedFileNode, FileNode};
use common::storage::index::{IndexValue, IndexableAndSerializableObject, IndexableObject};
use types::{acl::AclGrant, collection::SyncCollection};

impl IndexableObject for FileNode {
    fn index_values(&self) -> impl Iterator<Item = IndexValue<'_>> {
        let mut values = Vec::with_capacity(6);

        values.extend([
            IndexValue::Acl {
                value: (&self.acls).into(),
            },
            IndexValue::LogItem {
                prefix: None,
                sync_collection: SyncCollection::FileNode,
            },
            IndexValue::Quota { used: self.size() },
        ]);

        if let Some(file) = &self.file {
            values.extend([IndexValue::Blob {
                value: file.blob_hash.clone(),
            }]);
        }

        values.into_iter()
    }
}

impl IndexableObject for &ArchivedFileNode {
    fn index_values(&self) -> impl Iterator<Item = IndexValue<'_>> {
        let mut values = Vec::with_capacity(6);

        values.extend([
            IndexValue::Acl {
                value: self
                    .acls
                    .iter()
                    .map(AclGrant::from)
                    .collect::<Vec<_>>()
                    .into(),
            },
            IndexValue::LogItem {
                prefix: None,
                sync_collection: SyncCollection::FileNode,
            },
            IndexValue::Quota { used: self.size() },
        ]);

        if let Some(blob_hash) = self.file.blob_hash() {
            values.extend([IndexValue::Blob {
                value: blob_hash.into(),
            }]);
        }

        values.into_iter()
    }
}

impl IndexableAndSerializableObject for FileNode {
    fn is_versioned() -> bool {
        true
    }
}

impl FileNode {
    pub fn size(&self) -> u64 {
        self.dead_properties.size() as u64
            + self.display_name.as_ref().map_or(0, |n| n.len() as u64)
            + self.name.len() as u64
            + self.file.as_ref().map_or(0, |f| f.size)
            + std::mem::size_of::<FileNode>() as u64
    }
}

impl ArchivedFileNode {
    pub fn size(&self) -> u64 {
        self.dead_properties.size() as u64
            + self.display_name.as_ref().map_or(0, |n| n.len()) as u64
            + self.name.len() as u64
            + self.file.size().unwrap_or_default()
            + std::mem::size_of::<FileNode>() as u64
    }
}
