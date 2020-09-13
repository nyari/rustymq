use rand;
use core::serializer::{Serializable, Serializer, Deserializer};
use core::serializer;

#[derive(PartialEq)]
#[derive(Eq)]
#[derive(PartialOrd)]
#[derive(Ord)]
#[derive(Hash)]
#[derive(Clone)]
#[derive(Copy)]
pub struct Identifier
{
    core: u64  
}

impl Identifier {
    pub fn new(core: u64) -> Self {
        Self {
            core:core
        }
    }

    pub fn new_random() -> Self {
        Self {
            core: rand::random()
        }
    }

    pub fn get(&self) -> u64 {
        self.core.clone()
    }
}

pub struct VersionInfo {
    main: u16,
    sub: u16,
    rev: u16
}

impl VersionInfo {
    pub fn current() -> Self {
        Self {
            main: 0,
            sub: 0,
            rev: 0
        }
    }

    pub fn new(main: u16, sub: u16, rev: u16) -> Self {
        Self {
            main: main,
            sub: sub,
            rev: rev
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

pub struct UnixTime {
    unix_sec: u64,
    unix_ns: u32
}

pub struct UnixTimespan {
    sec: i64,
    ns: i32
}

impl Serializable for Identifier {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.core);
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize()?
        ))
    }
}

impl Serializable for VersionInfo {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.main);
        serializer.serialize(&self.sub);
        serializer.serialize(&self.rev);
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?
        ))
    }
}

pub mod thread {
    pub fn wait_for_success<T, F: Fn() -> Option<T>>(sleep_time: std::time::Duration, operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }

    pub fn wait_for_success_mut<T, F: FnMut() -> Option<T>>(sleep_time: std::time::Duration, mut operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }
}