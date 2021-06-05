use libc;

use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone)]
pub enum Error {
    EACCES,
    ELOOP,
    ENAMETOOLONG,
    ENOENT,
    EEXIST,
    ENOMEM,
    ENOSPC,
    ENOTDIR,
    EINVAL,
    ENFILE,
    EPERM,
    EIDRM,
    Unkown,
}

impl Error {
    pub fn from_libc_retval(value: libc::c_int) -> Self {
        match value {
            libc::EACCES => Self::EACCES,
            libc::ELOOP => Self::ELOOP,
            libc::ENAMETOOLONG => Self::ENAMETOOLONG,
            libc::ENOENT => Self::ENOENT,
            libc::EEXIST => Self::EEXIST,
            libc::ENOMEM => Self::ENOMEM,
            libc::ENOSPC => Self::ENOSPC,
            libc::ENOTDIR => Self::ENOTDIR,
            libc::EINVAL => Self::EINVAL,
            libc::ENFILE => Self::ENFILE,
            libc::EPERM => Self::EPERM,
            libc::EIDRM => Self::EIDRM,
            _ => Self::Unkown
        }
    }

    pub fn from_last_error() -> Self {
        Self::from_libc_retval(unsafe { *libc::__errno_location() })
    }

    pub fn parse(retval: libc::c_int) -> Result<libc::c_int, Self> {
        if retval != -1 { Ok(retval) } else { Err(Self::from_last_error()) }
    }

    pub fn parse_mut_ptr<T>(retval: * mut T) -> Result<* mut T, Self> {
        if retval != (usize::MAX as * mut T) { Ok(retval) } else { Err(Self::from_last_error()) }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SharedMemoryError {

}


#[derive(Clone)]
pub struct SharedMemoryHandle {
    id: ShmId,
    size: libc::size_t
}

impl SharedMemoryHandle {
    pub fn new_private(size: libc::size_t, flags: libc::c_int) -> Result<Self, Error> {
        Ok(Self {
            id: shmget(FToken(libc::IPC_PRIVATE), size, flags | libc::IPC_CREAT | libc::IPC_EXCL)?,
            size: size
        })
    }

    pub fn new(path: &str, proj_id: libc::c_int, size: libc::size_t, flags: libc::c_int) -> Result<Self, Error> {
        let key = ftok(path, proj_id)?;
        Ok(Self {
            id: shmget(key, size, flags)?,
            size: size
        })
    }

    pub fn get_mut(&self) -> Result<MutableSharedMemory, Error> {
        let mem = unsafe {shmat_rw(self.id)?};
        Ok(MutableSharedMemory{mem: mem, handle: self.clone()})
    }
}

pub struct MutableSharedMemory {
    mem: *mut u8,
    handle: SharedMemoryHandle
}

impl Clone for MutableSharedMemory {
    fn clone(&self) -> Self {
        self.handle.get_mut().unwrap()
    }
}

impl Drop for MutableSharedMemory {
    fn drop(&mut self) {
        unsafe {
            shmdt_rw(self.mem).expect("shmdt call with correct pointer failed");
        }
    }
}

impl Deref for MutableSharedMemory {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe {std::slice::from_raw_parts(self.mem, self.handle.size)}
    }
}

impl DerefMut for MutableSharedMemory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {std::slice::from_raw_parts_mut(self.mem, self.handle.size)}
    }
}

#[derive(Clone, Copy)]
struct FToken(libc::key_t);

#[derive(Clone, Copy)]
struct ShmId(libc::c_int);

fn ftok(path: &str, proj_id: libc::c_int) -> Result<FToken, Error> {
    Ok(FToken(Error::parse(unsafe{ libc::ftok(std::mem::transmute(path.as_ptr()), proj_id) })?))
}

fn shmget(key: FToken, size: libc::size_t, flags: libc::c_int) -> Result<ShmId, Error> {
    Ok(ShmId(Error::parse(unsafe {libc::shmget(key.0, size, flags)})?))
}

unsafe fn shmat_rw(id: ShmId) -> Result<* mut u8, Error> {
    Ok(std::mem::transmute(Error::parse_mut_ptr(libc::shmat(id.0, std::ptr::null(), 0))?))
}

unsafe fn shmdt_rw(mem: *mut u8) -> Result<(), Error> {
    Error::parse(libc::shmdt(std::mem::transmute(mem)))?;
    Ok(())
}