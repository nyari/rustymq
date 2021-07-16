const HEADER_MAGIC_VALUE: u16 = 0xDADA;
const MINIMUM_BUFFER_SIZE: usize = 0x400;
/// 1 kB

enum Error {
    MinimumBufferSizeNotReached,
}

#[repr(C)]
struct Header {
    pub counter: usize,
    pub first_header_offset: usize,
    pub magic_value: u16,
    pub has_first_message: bool,
}

impl Header {
    pub fn new() -> Self {
        Self {
            counter: 0,
            first_header_offset: 0,
            magic_value: HEADER_MAGIC_VALUE,
            has_first_message: false,
        }
    }
}

#[repr(C)]
struct SectionHeader {
    pub length: usize,
    pub counter: usize,
    pub complete: bool,
}

struct SharedMemoryPayloadBuffer<'a> {
    buffer: &'a mut [u8],
}

impl<'a> SharedMemoryPayloadBuffer<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Result<Self, Error> {
        if (buffer.len() >= MINIMUM_BUFFER_SIZE) {
            let mut result = Self { buffer: buffer };

            let header = result.header_mut();
            *header = Header::new();

            Ok(result)
        } else {
            Err(Error::MinimumBufferSizeNotReached)
        }
    }

    fn header(&self) -> &'a Header {
        let ptr = self.buffer.as_ptr();
        unsafe { &*std::mem::transmute::<*const u8, *const Header>(ptr) }
    }

    fn header_mut(&mut self) -> &'a mut Header {
        let ptr = self.buffer.as_mut_ptr();
        unsafe { &mut *std::mem::transmute::<*mut u8, *mut Header>(ptr) }
    }
}
