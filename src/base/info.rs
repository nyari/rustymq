//! # Information base module
//! Contains datastructures that contain operational information in RusyMQ 
use crate::internals::serializer;
use crate::internals::serializer::{Deserializer, Serializable, Serializer};

use rand;

/// # Identifier
/// Used as an identifier in messages as peer and other identifiers
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug)]
pub struct Identifier(u64);

impl Identifier {
    /// Create a new identifier from seed
    pub fn new(core: u64) -> Self {
        Self(core)
    }

    /// Create a random identifier
    pub fn new_random() -> Self {
        Self(rand::random())
    }

    /// Query internal state of identifier
    pub fn get(&self) -> u64 {
        self.0.clone()
    }
}

pub struct VersionInfo {
    main: u16,
    sub: u16,
    rev: u16,
}

impl VersionInfo {
    pub fn current() -> Self {
        Self {
            main: 0,
            sub: 0,
            rev: 0,
        }
    }

    pub fn new(main: u16, sub: u16, rev: u16) -> Self {
        Self {
            main: main,
            sub: sub,
            rev: rev,
        }
    }

    pub fn get(&self) -> (u16, u16, u16) {
        (self.main, self.sub, self.rev)
    }

    pub fn interface_compatible(&self, other: &Self) -> bool {
        self.main >= other.main
    }

    pub fn feature_compatible(&self, other: &Self) -> bool {
        self.main >= other.main
    }
}

impl Serializable for Identifier {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.0);
    }
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(deserializer.deserialize()?))
    }
}

impl Serializable for VersionInfo {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.main);
        serializer.serialize(&self.sub);
        serializer.serialize(&self.rev);
    }
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
        ))
    }
}
