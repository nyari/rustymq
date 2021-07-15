use libc;

use std::convert::TryInto;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::util::sync::Semaphore;

const SETVAL: libc::c_int = 16;

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
    EBUSY,
    EFBIG,
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
            libc::EBUSY => Self::EBUSY,
            libc::EFBIG => Self::EFBIG,
            _ => panic!("Unhandled libc error!")
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

#[derive(Clone, Copy)]
pub struct Permissions(bool, bool, bool);

impl Permissions {
    pub fn new_none() -> Self {
        Self(false, false, false)
    }

    pub fn new_read() -> Self {
        Self(true, false, false)
    }

    pub fn new_rw() -> Self {
        Self(true, true, false)
    }

    pub fn flags(&self) -> u16 {
        let mut result = 0u16;
        if self.0 {result |= 0b_100_u16} // Read
        if self.1 {result |= 0b_010_u16} // Write
        if self.2 {result |= 0b_001_u16} // Execute
        result
    }
}

#[derive(Clone, Copy)]
pub struct PermissionSet(Permissions, Permissions, Permissions);

impl PermissionSet {
    pub fn new_user_only(perms: Permissions) -> Self {
        Self(perms, Permissions::new_none(), Permissions::new_none())
    }

    pub fn new_user_and_group(perms: Permissions) -> Self {
        Self(perms, perms, Permissions::new_none())
    }

    pub fn new_everyone(perms: Permissions) -> Self {
        Self(perms, perms, perms)
    }

    pub fn flags(&self) -> u16 {
        self.0.flags() << 6u16 | self.1.flags() << 3u16 | self.2.flags() 
    }
}

#[derive(Clone)]
pub struct SharedMemoryBuilder {
    id: ShmId,
    size: libc::size_t
}

impl SharedMemoryBuilder {
    pub fn new_private(size: libc::size_t, perms: PermissionSet) -> Result<Self, Error> {
        Ok(Self {
            id: shmget(FToken(libc::IPC_PRIVATE), size, libc::IPC_CREAT | libc::IPC_EXCL | perms.flags() as i32)?,
            size: size
        })
    }

    pub fn new(path: &str, proj_id: libc::c_int, size: libc::size_t, perms: PermissionSet) -> Result<Self, Error> {
        let key = ftok(path, proj_id)?;
        Ok(Self {
            id: shmget(key, size, libc::IPC_CREAT | perms.flags() as i32)?,
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
    handle: SharedMemoryBuilder
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

pub struct SemaphoreSet {
    key: FToken,
    used_sems: Vec<std::sync::atomic::AtomicBool>
}

impl SemaphoreSet {
    pub fn new_private(nsems: libc::c_int, perms: PermissionSet) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            key: FToken(semget(FToken(libc::IPC_PRIVATE), nsems.clone(), libc::IPC_CREAT | libc::IPC_EXCL | perms.flags() as i32)?),
            used_sems: std::iter::repeat_with(|| std::sync::atomic::AtomicBool::new(false)).take(nsems as usize).collect()
        }))
    }

    pub fn new(path: &str, proj_id: libc::c_int, nsems: libc::c_int, perms: PermissionSet) -> Result<Arc<Self>, Error> {
        let token = ftok(path, proj_id)?;
        Ok(Arc::new(Self {
            key: FToken(semget(token, nsems, libc::IPC_CREAT | perms.flags() as i32)?),
            used_sems: std::iter::repeat_with(|| std::sync::atomic::AtomicBool::new(false)).take(nsems as usize).collect()
        }))
    }

    fn check_semaphore_num(&self, sem_num: libc::c_int) -> Result<(), Error> {
        if sem_num < 0 {
            Err(Error::EINVAL)
        } else if sem_num as usize > self.used_sems.len() {
            Err(Error::EFBIG)
        } else {
            Ok(())
        }
    }

    fn reserve_semaphore(&self, sem_num: libc::c_int) -> Result<(), Error> {
        self.check_semaphore_num(sem_num)?;
        match self.used_sems[sem_num as usize].compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(false) => Ok(()),
            Ok(true) => Err(Error::EBUSY),
            _ => Err(Error::EBUSY)
        }
    }

    fn release_semaphore(&self, sem_num: libc::c_int) -> Result<(), Error> {
        self.check_semaphore_num(sem_num)?;
        match self.used_sems[sem_num as usize].compare_exchange(true, false, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::EBUSY),
            _ => Err(Error::EBUSY)
        }
    }

    fn execute(&self, operations: &mut [SemaphoreOperation]) -> Result<(), Error> {
        semop(self.key, operations)
    }

    fn execute_timeout(&self, operations: &mut [SemaphoreOperation], duration: std::time::Duration) -> Result<(), Error> {
        semop_timeout(self.key, operations, duration)
    } 

    fn mutex_lock(&self, sem_num: libc::c_ushort) -> Result<(), Error> {
        self.execute(&mut [SemaphoreOperation::new_decrement(sem_num, 1)])
    }

    fn mutex_unlock(&self, sem_num: libc::c_ushort) -> Result<(), Error> {
        self.execute(&mut [SemaphoreOperation::new_increment(sem_num, 1)])
    }
}

pub struct SemaphoreMutex<T> {
    payload: T,
    semaphore_set: Arc<SemaphoreSet>,
    sem_num: libc::c_int
}

impl<T> SemaphoreMutex<T> {
    pub fn new(value: T, set: Arc<SemaphoreSet>, sem_num: libc::c_int) -> Result<Self, Error> {
        set.reserve_semaphore(sem_num)?;
        unsafe {
            Error::parse(semctl(set.key.0, sem_num, SETVAL, semun{val: 1}))?;
        }
        set.execute(&mut [SemaphoreOperation::new_increment(sem_num.try_into().unwrap() , 1)])?;
        Ok(Self {
            payload: value,
            semaphore_set: set,
            sem_num: sem_num
        })
    }

    pub fn lock<'a>(&'a self) -> Result<SemaphoreMutexLock<'a, T>, Error> {
        Ok(SemaphoreMutexLock::lock(self)?)
    }
}

pub struct SemaphoreMutexLock<'a, T> {
    payload_mutex: &'a SemaphoreMutex<T>
}

impl<'a, T> SemaphoreMutexLock<'a, T> {
    fn lock(mutex: &'a SemaphoreMutex<T>) -> Result<Self, Error> {
        mutex.semaphore_set.mutex_lock(mutex.sem_num.try_into().unwrap())?;
        Ok(Self {
            payload_mutex: mutex
        })
    }
}

impl<'a, T> Deref for SemaphoreMutexLock<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.payload_mutex.payload
    }
}

impl<'a, T> DerefMut for SemaphoreMutexLock<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let ptr = &self.payload_mutex.payload as *const T;
            let mut_ptr = ptr as *mut T;
            &mut *mut_ptr
        }
    }
}

impl<'a, T> Drop for SemaphoreMutexLock<'a, T> {
    fn drop(&mut self) {
        let sem_num = self.payload_mutex.sem_num;
        self.payload_mutex.semaphore_set.mutex_unlock(sem_num.try_into().unwrap()).unwrap();
    }
}

#[repr(C)]
struct SemaphoreOperation {
    sem_num: libc::c_ushort,
    sem_op: libc::c_short,
    sem_flags: libc::c_short
}

impl SemaphoreOperation {
    pub fn new_increment(sem_num: libc::c_ushort, amount: libc::c_ushort) -> Self {
        Self {
            sem_num: sem_num,
            sem_op: amount.try_into().unwrap(),
            sem_flags: 0
        }
    }

    pub fn new_decrement(sem_num: libc::c_ushort, amount: libc::c_ushort) -> Self {
        Self {
            sem_num: sem_num,
            sem_op: -(amount as libc::c_short),
            sem_flags: 0
        }
    }

    pub fn no_wait(self) -> Self {
        Self {
            sem_flags: self.sem_flags | libc::IPC_NOWAIT as libc::c_short,
            ..self
        }
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

fn semget(key: FToken, nsems: libc::c_int, flags: libc::c_int) -> Result<libc::c_int, Error> {
    unsafe { Error::parse(libc::semget(key.0, nsems, flags))}
}

fn semop(key: FToken, operations: &mut [SemaphoreOperation]) -> Result<(), Error> {
    unsafe { Error::parse(libc::semop(key.0, std::mem::transmute(operations.as_mut_ptr()), operations.len()))?;}
    Ok(())
}


fn semop_timeout(key: FToken, operations: &mut [SemaphoreOperation], duration: std::time::Duration) -> Result<(), Error> {
    let timespec = libc::timespec {
        tv_sec: duration.as_secs().try_into().unwrap(),
        #[cfg(all(target_arch = "x86_64", target_pointer_width = "32"))]
        tv_nsec: duration.duration.subsec_nanos().try_into().unwrap(),
        #[cfg(not(all(target_arch = "x86_64", target_pointer_width = "32")))]
        tv_nsec: duration.subsec_nanos().try_into().unwrap(),
    };
    unsafe { Error::parse(semtimedop(key.0, std::mem::transmute(operations.as_mut_ptr()), operations.len(), &timespec))?;}
    Ok(())
}

#[repr(C)]
struct semid_ds {
    sem_perm: libc::ipc_perm,
    sem_otime: libc::time_t,
    sem_ctime: libc::time_t,
    sem_nsems: libc::c_ulong
}

#[repr(C)]
union semun {
    val: libc::c_int,
    buf: *mut semid_ds,
    array: *mut libc::c_ushort,
    #[cfg(target_os="linux")]
    _dontcare: usize
}

extern "C" {
    fn semtimedop(
        semid: libc::c_int,
        sops: *mut libc::sembuf,
        nsops: libc::size_t,
        timeout: *const libc::timespec
    ) -> libc::c_int;

    fn semctl(semid: libc::c_int, semnum: libc::c_int, cmd: libc::c_int, extra: semun) -> libc::c_int;
}