/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod index;
pub mod storage;

use types::{acl::AclGrant, blob_hash::BlobHash, dead_property::DeadProperty};

#[derive(
    rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, Debug, Default, Clone, PartialEq, Eq,
)]
#[rkyv(derive(Debug))]
pub struct FileNode {
    pub parent_id: u32,
    pub name: String,
    pub display_name: Option<String>,
    #[rkyv(with = FilePropertyConverter)]
    pub file: Option<FileProperties>,
    pub created: i64,
    pub modified: i64,
    pub dead_properties: DeadProperty,
    pub acls: Vec<AclGrant>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileProperties {
    pub blob_hash: BlobHash,
    pub size: u64,
    pub media_type: Option<String>,
    pub executable: bool,
}

/// Serialized version of `Option<FileProperties>`, made to be backwards compatible with 32-bit file sizes.
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[repr(u8)]
pub enum UpgradableFileProperties {
    /// Was `ArchivedOption::None`
    None = 0,
    /// was `ArchivedOption::Some(ArchivedFileProperties)`
    Small {
        /// `blob_hash` must start at offset 4 for `ArchivedUpgradableFileProperties` to be backwards compatible with
        /// the historical 32-bit `ArchivedOption<ArchivedFileProperties>`. Since the `ArchivedFileProperties` was
        /// 4-byte-aligned, there needed to be padding between the enum discriminant and the payload, but since
        /// `BlobHash` is 1 byte align, the padding isn't done when defining the enum payload this way.
        ///
        /// These bytes **must be 0 at all times** or forward compatibility will break.
        _padding: [u8; 3],
        blob_hash: BlobHash,
        size: u32,
        media_type: Option<String>,
        executable: bool,
    } = 1,
    /// This is possible because the old `ArchivedOption<ArchivedFileProperties>` has 6 bytes of padding that we can
    /// use
    ///
    /// ```text
    /// [ 0.. 1] ArchivedOption<ArchivedFileProperties> discriminate (0=None, 1=Some)
    /// [ 1.. 4] (padding for ArchivedOption<ArchivedFileProperties>)
    /// [ 4..36] blob_hash: [u8; 32]
    /// [36..40] size: u32
    /// [40..41] media_type: ArchivedOption<ArchivedString>
    /// [41..44] (padding for ArchivedOption<ArchivedString>)
    /// [44..52] (payload for ArchivedOption<ArchivedString>, always take a minimum of 8 bytes, 4 byte aligned)
    /// [52..53] executable: bool
    /// [53..56] (padding)
    /// ```
    ///
    /// As you can see, there are 3 bytes between the first `Option` discriminate and another 3 bytes after the last
    /// `executable`. If we reorder some stuff around, we can put a `u64` in here without changing the serialized size
    /// of `ArchivedFileNode`, thus maintaining backwards compatibility.
    ///
    /// ```text
    /// [ 0.. 1] `ArchivedUpgradableFileProperties` discriminate (2=Large)
    /// [ 1..33] blob_hash: [u8; 32]
    /// [33..34] executable: bool
    /// [34..36] (padding)
    /// [36..37] media_type: ArchivedOption<ArchivedString>
    /// [37..40] (padding for ArchivedOption<ArchivedString>)
    /// [40..48] (payload for ArchivedOption<ArchivedString>)
    /// [48..56] size: u64
    /// ```
    Large {
        blob_hash: BlobHash,
        executable: bool,
        media_type: Option<String>,
        size: u64,
    } = 2,
}

impl From<&Option<FileProperties>> for UpgradableFileProperties {
    fn from(f: &Option<FileProperties>) -> Self {
        match f {
            None => UpgradableFileProperties::None,
            Some(p) if p.size <= u32::MAX as u64 => UpgradableFileProperties::Small {
                _padding: [0; 3],
                blob_hash: p.blob_hash.clone(),
                size: p.size as u32,
                media_type: p.media_type.clone(),
                executable: p.executable,
            },
            Some(p) => UpgradableFileProperties::Large {
                blob_hash: p.blob_hash.clone(),
                size: p.size,
                executable: p.executable,
                media_type: p.media_type.clone(),
            },
        }
    }
}

impl ArchivedUpgradableFileProperties {
    pub fn is_none(&self) -> bool {
        match self {
            Self::None => true,
            _ => false,
        }
    }
    pub fn size(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Small { size, .. } => Some(size.to_native() as u64),
            Self::Large { size, .. } => Some(size.to_native()),
        }
    }
    pub fn blob_hash(&self) -> Option<BlobHash> {
        match self {
            Self::None => None,
            Self::Small { blob_hash, .. } | Self::Large { blob_hash, .. } => {
                Some(BlobHash(blob_hash.0))
            }
        }
    }
    pub fn executable(&self) -> Option<bool> {
        match self {
            Self::None => None,
            Self::Small { executable, .. } | Self::Large { executable, .. } => Some(*executable),
        }
    }
    pub fn media_type(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Small { media_type, .. } | Self::Large { media_type, .. } => Some(
                media_type
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or("application/octet-stream"),
            ),
        }
    }
    pub fn get(&self) -> Option<FileProperties> {
        match self {
            Self::None => return None,
            Self::Small {
                _padding: _,
                blob_hash,
                executable,
                size,
                media_type,
            } => Some(FileProperties {
                blob_hash: BlobHash(blob_hash.0),
                size: size.to_native() as u64,
                media_type: media_type.as_ref().map(|mtype| mtype.as_str().to_owned()),
                executable: *executable,
            }),
            Self::Large {
                blob_hash,
                executable,
                size,
                media_type,
            } => Some(FileProperties {
                blob_hash: BlobHash(blob_hash.0),
                size: size.to_native(),
                media_type: media_type.as_ref().map(|mtype| mtype.as_str().to_owned()),
                executable: *executable,
            }),
        }
    }
}

pub struct FilePropertyConverter;

pub struct FilePropertyConverterResolver {
    repr: UpgradableFileProperties,
    inner: UpgradableFilePropertiesResolver,
}

impl rkyv::with::ArchiveWith<Option<FileProperties>> for FilePropertyConverter {
    type Archived = ArchivedUpgradableFileProperties;
    type Resolver = FilePropertyConverterResolver;

    fn resolve_with(
        _field: &Option<FileProperties>,
        resolver: Self::Resolver,
        out: rkyv::Place<Self::Archived>,
    ) {
        use rkyv::Archive as _;
        let FilePropertyConverterResolver { repr, inner } = resolver;
        repr.resolve(inner, out);
    }
}

impl<S> rkyv::with::SerializeWith<Option<FileProperties>, S> for FilePropertyConverter
where
    S: rkyv::rancor::Fallible + ?Sized,
    UpgradableFileProperties: rkyv::Serialize<S>,
{
    fn serialize_with(
        field: &Option<FileProperties>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        use rkyv::Serialize as _;
        let repr = UpgradableFileProperties::from(field);
        let inner = repr.serialize(serializer)?;
        Ok(FilePropertyConverterResolver { repr, inner })
    }
}

impl<D> rkyv::with::DeserializeWith<ArchivedUpgradableFileProperties, Option<FileProperties>, D>
    for FilePropertyConverter
where
    D: rkyv::rancor::Fallible + ?Sized,
{
    fn deserialize_with(
        field: &ArchivedUpgradableFileProperties,
        _: &mut D,
    ) -> Result<Option<FileProperties>, D::Error> {
        Ok(field.get())
    }
}

/*
// TODO: Write tests that prove that the new structs are backwards compatible when deserializing
mod v1 {
    use super::BlobHash;
    use rkyv::{Archive, Deserialize, Serialize};
    #[derive(Archive, Deserialize, Serialize, Debug, Default, Clone, PartialEq, Eq)]
    pub struct FileProperties {
        pub blob_hash: BlobHash,
        pub size: u32,
        pub media_type: Option<String>,
        pub executable: bool,
    }
    #[derive(Archive, Deserialize, Serialize, Debug, Default, Clone, PartialEq, Eq)]
    pub struct FileNode {
        pub parent_id: u32,
        pub name: String,
        pub display_name: Option<String>,
        pub file: Option<FileProperties>,
        pub created: i64,
        pub modified: i64,
        pub acls: Vec<u32>,
    }
}

*/
