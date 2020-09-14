use core::message::{PeerId, Message, RawMessage, MessageMetadata};
use core::util;
use core::transport::{Transport, InitiatorTransport, AcceptorTransport, TransportMethod};
use core::socket::{ConnectorError, SocketError, OpFlag};
use core::serializer;
use core::serializer::{FlatSerializer, FlatDeserializer, Serializer, Serializable, Buffer, BufferSlice};

use std::collections::{HashMap, VecDeque, HashSet};
use std::net;
use std::net::{SocketAddr};
use std::io;
use std::io::{Write, Read};
use std::thread;
use std::sync::{Arc, Mutex};
use std::convert::{From};

const BUFFER_BATCH_SIZE: usize = 2048;
const WAIT_TIME_MS: u64 = 128;

impl From<io::Error> for SocketError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::AddrInUse => SocketError::TransportMethodAlreadyInUse,
            io::ErrorKind::AddrNotAvailable => SocketError::TransportTargetUnreachable,
            io::ErrorKind::AlreadyExists => SocketError::TransportMethodAlreadyInUse,
            io::ErrorKind::BrokenPipe => SocketError::Disconnected,
            io::ErrorKind::ConnectionAborted => SocketError::Disconnected,
            io::ErrorKind::ConnectionRefused => SocketError::ConnectionRefused,
            io::ErrorKind::Interrupted => SocketError::Disconnected,
            io::ErrorKind::InvalidData => SocketError::InternalError,
            io::ErrorKind::InvalidInput => SocketError::InternalError,
            io::ErrorKind::NotConnected => SocketError::Disconnected,
            io::ErrorKind::TimedOut => SocketError::Timeout,
            io::ErrorKind::Other => SocketError::InternalError,
            _ => SocketError::InternalError,
        }
    }
}

enum TCPConnectionStreamState {
    Finished,
    Empty,
    Connection(SocketError)
}

impl From<io::Error> for TCPConnectionStreamState {
    fn from(error: io::Error) -> Self {
        TCPConnectionStreamState::Connection(SocketError::from(error))
    }
}

impl From<SocketError> for TCPConnectionStreamState {
    fn from(error: SocketError) -> Self {
        TCPConnectionStreamState::Connection(error)
    }
}

struct RawMessageBufferStreamReader {
    buffer: Buffer,
    batch_size: usize,
    len: usize,
    capacity: usize
}

impl RawMessageBufferStreamReader {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Buffer::with_capacity(batch_size),
            batch_size: batch_size,
            capacity: batch_size,
            len: 0
        }
    }

    pub fn try_parse_raw_message(&mut self) -> Result<RawMessage, SocketError> {
        let (mut deserializer, actual_bytes) = match FlatDeserializer::new(&self.buffer.as_slice()[..self.len]) {
            Ok(result) => Ok((result, self.len)),
            Err(serializer::Error::IncorrectBufferSize(actual_size)) => {
                if self.len < actual_size as usize {
                    Err(SocketError::IncompleteData)
                } else if self.len > actual_size as usize {
                    match FlatDeserializer::new(&self.buffer.as_slice()[..actual_size as usize]) {
                        Ok(result) => Ok((result, actual_size as usize)),
                        Err(_) => Err(SocketError::UnknownDataFormatReceived)
                    }
                } else {
                    Err(SocketError::UnknownDataFormatReceived)
                }
            }
            Err(serializer::Error::EndOfBuffer) => Err(SocketError::IncompleteData),
            _ => Err(SocketError::UnknownDataFormatReceived),
        }?;

        match RawMessage::deserialize(&mut deserializer) {
            Ok(message) => {
                self.buffer.copy_within(actual_bytes.., 0usize);
                self.len = self.len - actual_bytes;
                self.capacity = self.len;
                self.buffer.truncate(self.len);
                Ok(message)
            }
            Err(serializer::Error::DemarshallingFailed) | Err(serializer::Error::ByteOrderMarkError) => Err(SocketError::UnknownDataFormatReceived),
            Err(serializer::Error::EndOfBuffer) => Err(SocketError::InternalError),
            _ => panic!("Any other case should already have been handled")
        }
    }

    pub fn read_into<F: Read>(&mut self, reader:&mut F) -> Result<(), TCPConnectionStreamState>{
        self.ensure_batch_size_capacity_at_end_of_buffer();
        self.read_into_buffer(reader)?;
        Ok(())
    }

    fn ensure_batch_size_capacity_at_end_of_buffer<'a>(&'a mut self) {
        self.capacity = std::cmp::max(self.capacity, self.len + self.batch_size);
        if self.capacity > self.buffer.len() {
            self.buffer.resize(self.capacity - self.buffer.len(), 0u8);
        }
    }

    fn read_into_buffer<'a, F: Read>(&mut self, reader:&mut F) -> Result<(), TCPConnectionStreamState> {
        let read_amount = reader.read(&mut self.buffer.as_mut_slice()[self.len..self.batch_size])?;
        self.len += read_amount;
        Ok(())
    }
}

struct RawMessageBufferStreamWriter {
    buffer: Buffer,
    metadata: Option<MessageMetadata>,
    batch_size: usize,
    offset: usize
}

impl RawMessageBufferStreamWriter {
    pub fn new(message: RawMessage, batch_size: usize, store_metadata: bool) -> Self {
        Self {
            buffer: {let mut serializer = FlatSerializer::new();
                     serializer.serialize(&message);
                     serializer.finalize()},
            metadata: if store_metadata {Some(message.into_metadata())} else {None},
            batch_size: batch_size,
            offset: 0
        }
    }

    pub fn new_empty() -> Self {
        Self {
            buffer: Buffer::new(),
            metadata: None,
            batch_size: 0,
            offset: 0
        }
    }

    pub fn get_metadata(&self) -> &Option<MessageMetadata> {
        &self.metadata
    }

    fn get_batch<'a>(&'a self) -> Option<BufferSlice<'a>> {
        if self.offset + self.batch_size < self.buffer.len() {
            let offset = self.offset;
            Some(&self.buffer[offset..self.batch_size])
        } else if self.offset < self.buffer.len() {
            let offset = self.offset;
            Some(&self.buffer[offset..])
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.offset >= self.buffer.len()
    }

    fn progress_amount(&mut self, amount: usize) {
        if amount <= self.batch_size {
            self.offset += amount
        } else {
            panic!("Cannot push back more than batch size")
        }
    }

    pub fn write_into<F: Write>(&mut self, writer:&mut F)-> Result<(), TCPConnectionStreamState> {
        let result = {match self.get_batch() {
            Some(buffer_slice) => {
                Ok(writer.write(buffer_slice)?)
            },
            None => {
                Err(TCPConnectionStreamState::Empty)
            }
        }};

        if let Ok(amount) = result {
            self.progress_amount(amount);
        }
        if self.is_empty() {Err(TCPConnectionStreamState::Finished)} else { result.map(|_| {()}) }
    }
}

struct TCPConnectionStream {
    stream: net::TcpStream,
//    addr: SocketAddr,
    reader: RawMessageBufferStreamReader,
    writer: RawMessageBufferStreamWriter,
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
    inward_queue: Arc<Mutex<VecDeque<RawMessage>>>
}

impl TCPConnectionStream {
    pub fn connect(addr: SocketAddr) -> Result<Self, SocketError> {
        Ok(Self {
            stream: net::TcpStream::connect(addr)?,
//            addr: addr,
            reader: RawMessageBufferStreamReader::new(BUFFER_BATCH_SIZE),
            writer: RawMessageBufferStreamWriter::new_empty(),
            prio_outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            prio_sent_messages: Arc::new(Mutex::new(HashSet::new())),
            outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            inward_queue: Arc::new(Mutex::new(VecDeque::new()))
        })
    }

    pub fn get_handle(&self) -> TCPConnectionStreamHandle {
        TCPConnectionStreamHandle::new(self.outward_queue.clone(),
                                       self.prio_outward_queue.clone(),
                                       self.prio_sent_messages.clone(),
                                       self.inward_queue.clone())
    }

    fn get_outward_message_from_prio_queue(&mut self) -> Option<RawMessage> {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        prio_outward_queue.pop_front()
    }

    fn get_outward_message_from_normal_queue(&self) -> Option<RawMessage> {
        let mut outward_queue = self.outward_queue.lock().unwrap();
        outward_queue.pop_front()
    }

    pub fn start_next_message(&mut self) -> Result<(), TCPConnectionStreamState> {
        if let Some(message) = self.get_outward_message_from_prio_queue() {
            Ok(self.writer = RawMessageBufferStreamWriter::new(message, BUFFER_BATCH_SIZE, true))
        } else if let Some(message) = self.get_outward_message_from_normal_queue() {
            Ok(self.writer = RawMessageBufferStreamWriter::new(message, BUFFER_BATCH_SIZE, false))
        } else {
            Err(TCPConnectionStreamState::Empty)
        }
    }

    pub fn proceed_sending(&mut self) -> Result<(), TCPConnectionStreamState> {
        match self.writer.write_into(&mut self.stream) {
            Err(TCPConnectionStreamState::Finished) => {
                if let Some(metadata) = self.writer.get_metadata() {
                    let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
                    prio_sent_messages.insert(metadata.clone());
                }
                Err(TCPConnectionStreamState::Finished)
            }
            _other => _other
        }
    }

    pub fn proceed_receiving(&mut self) -> Result<(), TCPConnectionStreamState> {
        self.reader.read_into(&mut self.stream)?;
        match self.reader.try_parse_raw_message() {
            Ok(message) => {
                let mut inward_queue = self.inward_queue.lock().unwrap();
                inward_queue.push_back(message);
                Ok(())
            }
            Err(err) => Err(TCPConnectionStreamState::from(err))
        }
    }
}

struct TCPConnectionStreamHandle {
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
    inward_queue: Arc<Mutex<VecDeque<RawMessage>>>
}

impl TCPConnectionStreamHandle {
    pub fn new(outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
               prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
               prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
               inward_queue: Arc<Mutex<VecDeque<RawMessage>>>) -> TCPConnectionStreamHandle {
        Self {
            outward_queue: outward_queue,
            prio_outward_queue: prio_outward_queue,
            prio_sent_messages: prio_sent_messages,
            inward_queue: inward_queue
        }
    }

    pub fn add_to_outward_queue(&self, message: RawMessage) {
        let mut outward_queue = self.outward_queue.lock().unwrap();
        outward_queue.push_back(message)
    }

    fn add_to_prio_outward_queue(&self, message: RawMessage) {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        prio_outward_queue.push_back(message)
    }

    fn wait_for_message(&self, metadata: MessageMetadata) {
        util::thread::wait_for_success(std::time::Duration::from_millis(WAIT_TIME_MS), || {
            let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
            if prio_sent_messages.contains(&metadata) {
                prio_sent_messages.remove(&metadata);
                Some(())
            } else {
                None
            }
        })
    }

    pub fn send_now(&self, message: RawMessage) {
        let metadata = message.metadata().clone();
        self.add_to_prio_outward_queue(message);
        self.wait_for_message(metadata)
    }

    pub fn receive_async(&self) -> Option<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        inward_queue.pop_front()
    }

    pub fn receive(&self) -> Result<RawMessage, SocketError> {
        Ok(util::thread::wait_for_success(std::time::Duration::from_millis(WAIT_TIME_MS), || {
            self.receive_async()
        }))
    }
}

#[derive(PartialEq)]
enum TCPConnectionWorkerState {
    Busy,
    Free
}

impl TCPConnectionWorkerState {
    pub fn is_free(&self) -> bool {
        *self == TCPConnectionWorkerState::Free
    }
} 

struct TCPConnectionWorker {
    stream: TCPConnectionStream,
}

impl TCPConnectionWorker {
    pub fn connect(addr: SocketAddr) -> Result<(Self, TCPConnectionStreamHandle), SocketError> {
        let worker = Self {
            stream: TCPConnectionStream::connect(addr)?
        };
        let handle = worker.stream.get_handle();
        Ok((worker, handle))
    }

    fn process_receiving(&mut self) -> Result<TCPConnectionWorkerState, SocketError> {
        match self.stream.proceed_receiving() {
            Ok(()) => Ok(TCPConnectionWorkerState::Busy),
            Err(TCPConnectionStreamState::Empty) | Err(TCPConnectionStreamState::Finished) => {
                Ok(TCPConnectionWorkerState::Free)
            }
            Err(TCPConnectionStreamState::Connection(err)) => Err(err)
        }
    }

    fn process_sending(&mut self) -> Result<TCPConnectionWorkerState, SocketError> {
        match self.stream.proceed_sending() {
            Ok(()) => Ok(TCPConnectionWorkerState::Busy),
            Err(TCPConnectionStreamState::Empty) | Err(TCPConnectionStreamState::Finished) => {
                if let Err(TCPConnectionStreamState::Empty) = self.stream.start_next_message() {
                    Ok(TCPConnectionWorkerState::Free)
                } else {
                    Ok(TCPConnectionWorkerState::Busy)
                }
            }
            Err(TCPConnectionStreamState::Connection(err)) => Err(err)
        }
    }

    pub fn main_loop(mut self, stop_semaphore: Arc<Mutex<bool>>) -> Result<(), SocketError> {
        loop {
            loop {
                let receiving = self.process_receiving()?;
                let sending = self.process_sending()?;
                if receiving.is_free() && sending.is_free() {
                    break;
                }
            }
            if *stop_semaphore.lock().unwrap() {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(WAIT_TIME_MS));
        }
    }
}

struct TCPConnection {
    handle: TCPConnectionStreamHandle,
    addr: SocketAddr,
    worker_thread: Option<std::thread::JoinHandle<Result<(), SocketError>>>,
    stop_semaphore: Arc<Mutex<bool>>
}

impl TCPConnection {
    pub fn connect(addr: SocketAddr) -> Result<Self, SocketError> {
        let (worker, handle) = TCPConnectionWorker::connect(addr)?;
        let stop_semaphore = Arc::new(Mutex::new(false));
        let stop_semaphore_clone = stop_semaphore.clone();
        Ok(Self {
            handle: handle,
            addr: addr,
            worker_thread: Some(thread::spawn(move || { worker.main_loop(stop_semaphore) } )),
            stop_semaphore: stop_semaphore_clone
        })
    }

    pub fn send_async(&self, message: RawMessage) {
        self.handle.add_to_outward_queue(message)
    }

    pub fn send(&self, message: RawMessage) {
        self.handle.send_now(message);
    }

    pub fn receive_async(&self) -> Option<RawMessage> {
        self.handle.receive_async()
    }

    #[allow(dead_code)]
    pub fn receive(&self) -> Result<RawMessage, SocketError> {
        self.handle.receive()
    }

    pub fn get_address(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for TCPConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        {
            let mut stop_semaphore = self.stop_semaphore.lock().unwrap();
            *stop_semaphore = true;
        }
        self.worker_thread.take().unwrap().join().unwrap();
    }
}

struct TCPConnectionManager {
    peers: HashMap<PeerId, TCPConnection>,
    addresses: HashMap<SocketAddr, PeerId>,
    inward_queue: VecDeque<RawMessage>
}

impl TCPConnectionManager {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            addresses: HashMap::new(),
            inward_queue: VecDeque::new()
        }
    }

    pub fn connect(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        if !self.is_already_connected(&address) {
            self.connect_internal(address)
        } else {
            Err(ConnectorError::AlreadyConnected)
        }
    }

    pub fn get_message_peer_connection<'a>(&'a self, message: &RawMessage) -> Result<&'a TCPConnection, SocketError> {
        match message.peer_id() {
            Some(peerid) => match self.peers.get(peerid) {
                Some(connection) => Ok(connection),
                None => Err(SocketError::UnknownPeer)
            },
            None => if self.peers.len() == 1 {
                Ok(self.peers.values().next().unwrap())
            } else {
                Err(SocketError::UnknownPeer)
            }
        }
    }

    pub fn transform_received_message(&self, message: RawMessage, address: SocketAddr) -> RawMessage {
        message.apply_peer_id(self.addresses.get(&address).unwrap().clone())
    }

    fn connect_internal(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        match TCPConnection::connect(address) {
            Ok(connection) => {
                let peerid = PeerId::new_random();

                self.peers.insert(peerid, connection);
                self.addresses.insert(address, peerid);

                Ok(peerid)
            }
            Err(_) => Err(ConnectorError::CouldNotConnect)
        }
    }

    fn receive_from_all_connections(&mut self){
        let mut results = Vec::new();
        self.peers.values().for_each(|connection| {
            while let Some(message) = connection.receive_async() {
                results.push(self.transform_received_message(message, connection.get_address()))
            }
        });
        self.inward_queue.extend(results.into_iter())
    }

    fn is_already_connected(&self, address: &SocketAddr) -> bool {
        self.peers.values().find(|x| x.get_address() == *address).is_some()
    }
}

impl Transport for TCPConnectionManager {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        let connection = self.get_message_peer_connection(&message)?;
        match flags {
            OpFlag::Default => {connection.send(message); Ok(())},
            OpFlag::NoWait => Ok(connection.send_async(message)),
            opflag => Err(SocketError::UnsupportedOpFlag(opflag))
        }
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, SocketError> {
        self.receive_from_all_connections();
        match flags {
            OpFlag::Default => {
                if let Some(message) = self.inward_queue.pop_front() {
                    Ok(message)
                } else {
                    Ok(util::thread::wait_for_success_mut(std::time::Duration::from_millis(WAIT_TIME_MS), || {
                        self.receive_from_all_connections();
                        self.inward_queue.pop_front()
                    }))
                }
            }
            OpFlag::NoWait => {
                self.inward_queue.pop_front().ok_or(SocketError::Timeout)
            },
            opflag => Err(SocketError::UnsupportedOpFlag(opflag))
        }
    }

    fn close(self) -> Result<(), SocketError> {
        Err(SocketError::Timeout)
    }
}

pub struct TCPInitiatorTransport {
    manager: TCPConnectionManager
}

impl TCPInitiatorTransport {
    pub fn new() -> Self {
        Self {
            manager: TCPConnectionManager::new()
        }
    }
}

impl Transport for TCPInitiatorTransport {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, SocketError> {
        self.manager.receive(flags)
    }

    fn close(self) -> Result<(), SocketError> {
        self.manager.close()
    }
}

impl InitiatorTransport for TCPInitiatorTransport {
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        match target {
            TransportMethod::Network(address) => Ok(Some(self.manager.connect(address)?)),
            _ => Err(ConnectorError::InvalidTransportMethod)
        }
    }
}

pub struct TCPAcceptorTransport {
    manager: TCPConnectionManager
}