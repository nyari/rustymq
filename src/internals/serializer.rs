//! # Serializer module
//! ## Summary
//! This module contains a basic serialization functionality used internally by RusyMQ
//! But it can be used externally for serialization as a basic tool for serialization.
//! ## Details
//! The reason not to use a serialization library was to keep the dependencies of the library minimal
//! and also to have a simple and fast binary serialization that also supports big and little endiannes

/// General buffer type to be used in RusyMQ
pub type Buffer = Vec<u8>;
/// General buffer slice type to be used in RustyMQ
pub type BufferSlice<'a> = &'a [u8];
/// General buffer mutable slice type to be used in RustyMQ
pub type BufferMutSlice<'a> = &'a mut [u8];

/// Byte order mark value for serialization
const BOM_VALUE: u16 = 0xA55A;
/// Changed byte order mark value for serialization
const BOM_CHANGED_VALUE: u16 = 0x5AA5;

/// Signaling serialization errors
#[derive(Debug)]
pub enum Error {
    /// Error occured while demarshalling an input buffer during deserialization
    DemarshallingFailed,
    /// Byte order mark damaged
    ByteOrderMarkError,
    /// The buffer passed for deserialization does not match the required size. The stored value contains the size expected
    IncorrectBufferSize(u64),
    /// The buffer ended before the metadata on the beginning of the serialization could be parsed
    EndOfBuffer,
}

/// Serializer trait that can serialize a [`Serializable`] object
pub trait Serializer: Sized {
    /// Append a slice slice to the buffer stored in the serializer
    fn append<'a>(&mut self, slice: BufferSlice<'a>);

    /// Finalize the serializer and return the buffer containing the serialized data
    fn finalize(self) -> Buffer;

    /// Serialize [`Serializable`] object by reference
    #[inline]
    fn serialize<T: Serializable>(&mut self, serializable: &T) {
        serializable.serialize(self);
    }

    /// Serialize [`Serializable`] object by passing ownership
    #[inline]
    fn serialize_pass<T: Serializable>(&mut self, serializable: T) {
        self.serialize(&serializable)
    }

    /// Serizalize [`RawSerializable`] value
    #[inline]
    fn serialize_raw<T: RawSerializable>(&mut self, serializable: &T) {
        self.append(sized_to_byte_slice(serializable));
    }
    /// Serizalize a slice of [`RawSerializable`] objects
    #[inline]
    fn serialize_raw_slice<T: RawSerializable>(&mut self, slice: &[T]) {
        self.serialize_raw(&(slice.len() as u64));
        self.append(sized_slice_to_byte_slice(slice));
    }
}

/// Deserializer trait that can deserialize a buffer containining [`Serializable`] objects
pub trait Deserializer: Sized {
    /// Consume an amount of the the buffer being parsed by the deserializer
    fn consume<'a>(&'a mut self, amount: usize) -> Result<BufferSlice<'a>, Error>;
    /// Deserialize a [`Serializable`] object from the buffer being parsed by the deserializer
    fn deserialize<T: Serializable>(&mut self) -> Result<T, Error>;
    /// Check if byte order correction is needed on [`RawSerializable`] values being deserialized
    fn byte_order_correction(&self) -> bool;

    /// Deserialize a [`RawSerializable`] value from the buffer being parsed by the deserializer
    #[inline]
    fn deserialize_raw<T: RawSerializable>(&mut self) -> Result<T, Error> {
        let slice = self.consume(std::mem::size_of::<T>())?;
        unsafe {
            let result: *const T = std::mem::transmute(slice.as_ptr());
            Ok(if self.byte_order_correction() {
                (*result).swap_bytes()
            } else {
                *result
            })
        }
    }

    /// Deserialize a slice of [`RawSerializable`] values from the buffer being parsed by the deserializer
    fn deserialize_raw_slice<T: RawSerializable>(&mut self) -> Result<Vec<T>, Error> {
        let length = (self.deserialize_raw::<u64>()?) as usize;
        let byte_slice = self.consume(std::mem::size_of::<T>() * length)?;
        let slice: &[T] = unsafe {
            let pointer: *const T = std::mem::transmute(byte_slice.as_ptr());
            std::slice::from_raw_parts(pointer, length)
        };

        let mut result: Vec<T> = Vec::with_capacity(length);
        result.extend_from_slice(slice);

        if std::mem::size_of::<T>() > 1 && self.byte_order_correction() {
            for element in result.iter_mut() {
                *element = (*element).swap_bytes()
            }
        }

        Ok(result)
    }
}

/// Trait that enables a [`Serializer`] object can serialize and a [`Deserializer`] object can deserialize
pub trait Serializable
where
    Self: Sized,
{
    /// Serialize `self` into `serializer`
    fn serialize<T: Serializer>(&self, serializer: &mut T);
    /// Deserialize a value with the type of `Self` from a deserializer
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, Error>;
}

/// Trait that marks primitive types that can be serialized with [`Serializer`] object
pub trait RawSerializable
where
    Self: Sized + Copy,
{
    fn swap_bytes(self) -> Self;
}

/// Implementation for serializing an [`Option`] given that the contained value is a [`Serializable`]
impl<U: Serializable> Serializable for Option<U>
where
    U: Serializable,
{
    #[inline]
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        match self {
            Some(value) => {
                serializer.serialize(&1u8);
                serializer.serialize(value);
            }
            None => {
                serializer.serialize(&0u8);
            }
        }
    }

    #[inline]
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
        match deserializer.deserialize::<u8>()? {
            1 => match deserializer.deserialize::<U>() {
                Ok(value) => Ok(Some(value)),
                Err(error) => Err(error),
            },
            0 => Ok(None),
            _ => Err(Error::DemarshallingFailed),
        }
    }
}

/// Implementation for [`Serializer`] that stores values in a flat binary buffer
pub struct FlatSerializer {
    buffer: Buffer,
}

impl FlatSerializer {
    /// Create a new FlatSerializer
    pub fn new() -> Self {
        Self { buffer: Vec::new() }.put_bom().put_size_placeholder()
    }

    fn put_bom(mut self) -> Self {
        self.serialize(&BOM_VALUE);
        self
    }

    fn put_size_placeholder(mut self) -> Self {
        self.serialize_pass(0 as u64);
        self
    }

    fn write_size_to_placeholder(&mut self) {
        splice_slice(
            sized_to_byte_slice(&(self.buffer.len() as u64)),
            &mut self.buffer.as_mut_slice()[2..10],
        )
        .expect("FlatSerializer: Internal error")
    }
}

impl Serializer for FlatSerializer {
    #[inline]
    fn append<'a>(&mut self, slice: BufferSlice<'a>) {
        self.buffer.extend_from_slice(slice)
    }

    fn finalize(mut self) -> Buffer {
        self.write_size_to_placeholder();
        self.buffer
    }
}

/// Implementation for [`Deserializer`] that restores objects from a flat binary buffer
pub struct FlatDeserializer<'a> {
    buffer: BufferSlice<'a>,
    offset: usize,
    swap_byte_order: bool,
}

impl<'a> FlatDeserializer<'a> {
    /// Create a new FlatDeserializer that that will parse the [`BufferSlice`] given as parameter
    pub fn new(buffer: BufferSlice<'a>) -> Result<Self, Error> {
        let mut result = Self {
            buffer: buffer,
            offset: 0,
            swap_byte_order: false,
        };

        let mut result = match result.deserialize::<u16>()? {
            BOM_VALUE => Ok(result),
            BOM_CHANGED_VALUE => {
                result.swap_byte_order = true;
                Ok(result)
            }
            _ => Err(Error::ByteOrderMarkError),
        }?;

        let expected_buffer_size = result.deserialize::<u64>()?;
        if expected_buffer_size != buffer.len() as u64 {
            Err(Error::IncorrectBufferSize(expected_buffer_size))
        } else {
            Ok(result)
        }
    }
}

impl<'a> Deserializer for FlatDeserializer<'a> {
    #[inline]
    fn consume<'b>(&'b mut self, amount: usize) -> Result<BufferSlice<'b>, Error> {
        if amount + self.offset <= self.buffer.len() {
            let offset = self.offset;
            self.offset += amount;
            Ok(&self.buffer[offset..self.offset])
        } else {
            Err(Error::EndOfBuffer)
        }
    }

    #[inline]
    fn deserialize<T: Serializable>(&mut self) -> Result<T, Error> {
        let offset = self.offset;
        let result = T::deserialize(self);
        if let Err(_) = result {
            self.offset = offset;
        }
        result
    }

    #[inline]
    fn byte_order_correction(&self) -> bool {
        self.swap_byte_order
    }
}

#[inline]
fn sized_to_byte_slice<'a, T: Sized>(value: &'a T) -> BufferSlice<'a> {
    unsafe { std::slice::from_raw_parts(std::mem::transmute(&*value), std::mem::size_of::<T>()) }
}

#[inline]
fn sized_slice_to_byte_slice<'a, T: Sized>(value: &'a [T]) -> BufferSlice<'a> {
    unsafe {
        std::slice::from_raw_parts(
            std::mem::transmute(value.as_ptr()),
            std::mem::size_of::<T>() * value.len(),
        )
    }
}

#[derive(Debug)]
enum SpliceSliceError {
    SourceSliceLengthMismatch,
}

#[inline]
fn splice_slice<'a, T: Sized + Copy>(
    source: &'a [T],
    target: &'a mut [T],
) -> Result<(), SpliceSliceError> {
    if source.len() == target.len() {
        target.copy_from_slice(source);
        Ok(())
    } else {
        Err(SpliceSliceError::SourceSliceLengthMismatch)
    }
}

macro_rules! raw_serializable_impl {
    ($T:ty) => {
        impl Serializable for $T {
            #[inline]
            fn serialize<T: Serializer>(&self, serializer: &mut T) {
                serializer.serialize_raw(self);
            }

            #[inline]
            fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
                deserializer.deserialize_raw::<Self>()
            }
        }

        impl RawSerializable for $T {
            fn swap_bytes(self) -> Self {
                self.swap_bytes()
            }
        }
    };
}

raw_serializable_impl!(u8);
raw_serializable_impl!(u16);
raw_serializable_impl!(u32);
raw_serializable_impl!(u64);
raw_serializable_impl!(i8);
raw_serializable_impl!(i16);
raw_serializable_impl!(i32);
raw_serializable_impl!(i64);

#[cfg(test)]
mod tests {
    use super::*;

    fn is_little_endian() -> bool {
        sized_to_byte_slice(&BOM_VALUE)[0] == 0x5A
    }

    #[test]
    fn flat_serializer_1byte_test() {
        let mut ser = FlatSerializer::new();
        ser.serialize(&(0xAA as u8));
        let result = ser.finalize();
        if is_little_endian() {
            assert_eq!(
                result,
                [0x5A, 0xA5, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAA]
            );
        } else {
            assert_eq!(
                result,
                [0xA5, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0xAA]
            );
        }
    }

    #[test]
    fn flat_deserializer_1byte_test() {
        let buffer: [u8; 11] = if is_little_endian() {
            [
                0x5A, 0xA5, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAA,
            ]
        } else {
            [
                0xA5, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0xAA,
            ]
        };
        let mut ser = FlatDeserializer::new(&buffer).unwrap();
        assert_eq!(0xAA, ser.deserialize::<u8>().unwrap());
    }

    #[test]
    fn flat_deserializer_shorter() {
        let buffer: [u8; 10] = if is_little_endian() {
            [0x5A, 0xA5, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        } else {
            [0xA5, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00]
        };
        assert!(matches!(
            FlatDeserializer::new(&buffer),
            Err(Error::IncorrectBufferSize(11))
        ))
    }

    #[test]
    fn flat_deserializer_header_missing_partly() {
        let buffer: [u8; 8] = if is_little_endian() {
            [0x5A, 0xA5, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00]
        } else {
            [0xA5, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        };
        assert!(matches!(
            FlatDeserializer::new(&buffer),
            Err(Error::EndOfBuffer)
        ))
    }

    #[test]
    fn flat_deserializer_bom_incorrect() {
        let buffer: [u8; 8] = if is_little_endian() {
            [0x5A, 0xA6, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00]
        } else {
            [0xA6, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        };
        assert!(matches!(
            FlatDeserializer::new(&buffer),
            Err(Error::ByteOrderMarkError)
        ));
    }

    #[test]
    fn flat_deserializer_bom_correction_test() {
        let buffer: [u8; 14] = if is_little_endian() {
            [
                0xA5, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0E, 0xA3, 0xA2, 0xA1, 0xA0,
            ]
        } else {
            [
                0x5A, 0xA5, 0x0E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xA0, 0xA1, 0xA2, 0xA3,
            ]
        };

        let mut ser = FlatDeserializer::new(&buffer).unwrap();
        assert_eq!(ser.deserialize::<u32>().unwrap(), 0xA3A2A1A0);
    }

    #[test]
    fn flat_deserializer_empty() {
        let buffer: [u8; 0] = [];
        assert!(matches!(
            FlatDeserializer::new(&buffer),
            Err(Error::EndOfBuffer)
        ))
    }
}
