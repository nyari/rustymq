use core::message::{PeerId, Message, RawMessage, MessageMetadata};
use core::util;
use core::util::thread::{StoppedSemaphore};
use core::util::time::{LinearDurationBackoff, DurationBackoff, DurationBackoffWithDebounce};
use core::transport::{Transport, InitiatorTransport, AcceptorTransport, TransportMethod};
use core::socket::{ConnectorError, SocketError, OpFlag};
use stream;

use std::collections::{HashMap, VecDeque, HashSet};
use std::net;
use std::net::{SocketAddr};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use std::cell::{RefCell};

const BUFFER_BATCH_SIZE: usize = 2048;
const SOCKET_READ_TIMEOUT_MS: u64 = 16;

fn query_thread_default_duration_backoff() -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(LinearDurationBackoff::new(
        Duration::from_millis(0),
        Duration::from_millis(500),
        20), 500000)
}

fn query_acceptor_thread_default_duration_backoff() -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(LinearDurationBackoff::new(
        Duration::from_millis(0),
        Duration::from_millis(100),
        10), 100)
}

struct TCPConnectionStream {
    stream: net::TcpStream,
//    addr: SocketAddr,
    reader: stream::RawMessageReader,
    writer: stream::RawMessageWriter,
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
    inward_queue: Arc<Mutex<VecDeque<RawMessage>>>
}

impl TCPConnectionStream {
    pub fn connect(addr: SocketAddr) -> Result<Self, SocketError> {
        let stream = net::TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(Self {
            stream: stream,
//            addr: addr,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
            prio_outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            prio_sent_messages: Arc::new(Mutex::new(HashSet::new())),
            outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            inward_queue: Arc::new(Mutex::new(VecDeque::new()))
        })
    }

    pub fn accept_connection((stream, _addr): (net::TcpStream, SocketAddr)) -> Result<Self, SocketError> {
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(Self {
            stream: stream,
//            addr: addr,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
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

    pub fn start_next_message(&mut self) -> Result<(), stream::State> {
        if let Some(message) = self.get_outward_message_from_prio_queue() {
            Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE, true))
        } else if let Some(message) = self.get_outward_message_from_normal_queue() {
            Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE, false))
        } else {
            Err(stream::State::Empty)
        }
    }

    pub fn proceed_sending(&mut self) -> Result<(), stream::State> {
        match self.writer.write_into(&mut self.stream) {
            Err(stream::State::Empty) => {
                if let Some(metadata) = self.writer.get_metadata() {
                    let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
                    prio_sent_messages.insert(metadata.clone());
                }
                Err(stream::State::Empty)
            }
            _other => _other
        }
    }

    pub fn proceed_receiving(&mut self) -> Result<(), stream::State> {
        let messages = self.reader.read_into(&mut self.stream)?;
        let mut inward_queue = self.inward_queue.lock().unwrap();
        messages.into_iter().for_each(|message| {
            inward_queue.push_back(message);
        });
        Ok(())
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

    pub fn add_to_prio_outward_queue(&self, message: RawMessage) {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        prio_outward_queue.push_back(message)
    }

    pub fn check_prio_message_sent(&self, metadata: &MessageMetadata) -> bool {
        let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
        prio_sent_messages.take(metadata).is_some()
    }

    pub fn receive_async_all(&self) -> Vec<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        inward_queue.drain(..).collect()
    }
}

#[derive(Debug)]
enum TCPConnectionWorkerState {
    Busy,
    Free
}

impl TCPConnectionWorkerState {
    pub fn is_free(&self) -> bool {
        matches!(self, TCPConnectionWorkerState::Free)
    }
}

struct TCPConnectionWorker {
    stream: TCPConnectionStream,
}

impl TCPConnectionWorker {
    fn construct_from_stream(stream: TCPConnectionStream) -> Result<(Self, TCPConnectionStreamHandle), SocketError> {
        let worker = Self {
            stream: stream
        };
        let handle = worker.stream.get_handle();
        Ok((worker, handle))
    }

    pub fn connect(addr: SocketAddr) -> Result<(Self, TCPConnectionStreamHandle), SocketError> {
        Self::construct_from_stream(TCPConnectionStream::connect(addr)?)
    }

    pub fn accept_connection((stream, addr): (net::TcpStream, SocketAddr)) -> Result<(Self, TCPConnectionStreamHandle), SocketError> {
        Self::construct_from_stream(TCPConnectionStream::accept_connection((stream, addr))?)
    }

    fn process_receiving(&mut self) -> Result<TCPConnectionWorkerState, SocketError> {
        match self.stream.proceed_receiving() {
            Ok(()) => Ok(TCPConnectionWorkerState::Busy),
            Err(stream::State::Remainder) => Ok(TCPConnectionWorkerState::Busy),
            Err(stream::State::Empty) => {
                Ok(TCPConnectionWorkerState::Free)
            }
            Err(stream::State::Stream(err)) => Err(err)
        }
    }

    fn process_sending(&mut self) -> Result<TCPConnectionWorkerState, SocketError> {
        match self.stream.proceed_sending() {
            Ok(()) => Ok(TCPConnectionWorkerState::Busy),
            Err(stream::State::Empty) => {
                if let Err(stream::State::Empty) = self.stream.start_next_message() {
                    Ok(TCPConnectionWorkerState::Free)
                } else {
                    Ok(TCPConnectionWorkerState::Busy)
                }
            }
            Err(stream::State::Stream(err)) => Err(err),
            Err(stream::State::Remainder) => panic!("Internal error")
        }
    }

    pub fn main_loop(mut self, stop_semaphore: StoppedSemaphore) -> Result<(), SocketError> {
        let mut sleep_backoff = query_thread_default_duration_backoff();
        loop {
            loop {
                let receiving = self.process_receiving()?;
                let sending = self.process_sending()?;
                if receiving.is_free() && sending.is_free() {
                    break;
                } else {
                    sleep_backoff.reset();
                }
            }
            if stop_semaphore.is_stopped() {
                return Ok(());
            }
            std::thread::sleep(sleep_backoff.step());
        }
    }
}

struct TCPConnection {
    handle: TCPConnectionStreamHandle,
    addr: SocketAddr,
    worker_thread: RefCell<Option<std::thread::JoinHandle<Result<(), SocketError>>>>,
    stop_semaphore: StoppedSemaphore
}

impl TCPConnection {
    fn construct_from_worker_handle((worker, handle): (TCPConnectionWorker, TCPConnectionStreamHandle), addr: SocketAddr) -> Result<Self, SocketError> {
        let stop_semaphore = StoppedSemaphore::new();
        let stop_semaphore_clone = stop_semaphore.clone();
        Ok(Self {
            handle: handle,
            addr: addr,
            worker_thread: RefCell::new(Some(thread::spawn(move || { worker.main_loop(stop_semaphore.clone()) } ))),
            stop_semaphore: stop_semaphore_clone
        })
    }

    fn check_worker_state(&self) -> Result<(), SocketError> {
        if self.stop_semaphore.is_stopped() {
            match self.worker_thread.borrow_mut().take().unwrap().join() {
                Ok(Ok(())) => panic!("A worker should not exit without an error condition except when it is explicitly stopped by the semaphore"),
                Ok(error) => error,
                Err(_) => Err(SocketError::InternalError)
            }
        } else {
            Ok(())
        }
    }

    pub fn connect(addr: SocketAddr) -> Result<Self, SocketError> {
        Self::construct_from_worker_handle(TCPConnectionWorker::connect(addr)?, addr)
    }

    pub fn accept_connection((stream, addr): (net::TcpStream, SocketAddr)) -> Result<Self, SocketError> {
        Self::construct_from_worker_handle(TCPConnectionWorker::accept_connection((stream, addr))?, addr)
    }

    pub fn send_async(&self, message: RawMessage) -> Result<(), SocketError> {
        self.check_worker_state()?;
        self.handle.add_to_outward_queue(message);
        Ok(())
    }

    pub fn send(&self, message: RawMessage) -> Result<(), SocketError> {
        self.check_worker_state()?;
        let metadata = message.metadata().clone();
        self.handle.add_to_prio_outward_queue(message);
        util::thread::wait_for_backoff(query_thread_default_duration_backoff(), || {
            if let Err(err) = self.check_worker_state() {
                Some(Err(err))
            } else if self.handle.check_prio_message_sent(&metadata) {
                Some(Ok(()))
            } else {
                None
            }
        })
    }

    pub fn receive_async_all(&self) -> (Vec<RawMessage>, Option<SocketError>) {
        let messages = self.handle.receive_async_all();
        match self.check_worker_state() {
            Ok(()) => (messages, None),
            Err(err) => (messages, Some(err))
        }
    }

    pub fn get_address(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for TCPConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.stop_semaphore.stop();
        match self.worker_thread.borrow_mut().take() {
            Some(join_handle) => { join_handle.join(); },
            None => ()
        }
    }
}

struct TCPConnectionManagerPeers {
    peers: HashMap<PeerId, TCPConnection>,
    addresses: HashMap<SocketAddr, PeerId>,
}

impl TCPConnectionManagerPeers {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            addresses: HashMap::new()
        }
    }

    pub fn connect(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        if !self.is_already_connected(&address) {
            self.connect_internal(address)
        } else {
            Err(ConnectorError::AlreadyConnected)
        }
    }

    pub fn accept_connection(&mut self, (stream, addr): (net::TcpStream, SocketAddr)) -> Result<PeerId, ConnectorError> {
        match TCPConnection::accept_connection((stream, addr)) {
            Ok(connection) => self.commit_onnection(connection),
            Err(_) => Err(ConnectorError::CouldNotConnect)
        }
    }

    pub fn send_message(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        let connection = self.get_message_peer_connection(message.peer_id())?;
        match flags {
            OpFlag::Default => connection.send(message),
            OpFlag::NoWait => connection.send_async(message),
            opflag => Err(SocketError::UnsupportedOpFlag(opflag))
        }
    }

    pub fn receive_from_all_connections(&self) -> Vec<Result<RawMessage, (PeerId, SocketError)>> {
        let mut results = Vec::new();
        self.peers.values().for_each(|connection| {
            match connection.receive_async_all() {
                (messages, None) => {
                    results.extend(messages.into_iter().map(|message| { Ok(self.set_peer_id_on_received_message(message, connection.get_address())) }))
                },
                (messages, Some(err)) => {
                    results.extend(messages.into_iter().map(|message| { Ok(self.set_peer_id_on_received_message(message, connection.get_address())) }));
                    results.push(Err((self.get_peer_id_for_address(connection.get_address()), err)))
                }
            }
        });
        results
    }

    fn get_message_peer_connection<'a>(&'a self, peer_id: &Option<PeerId>) -> Result<&'a TCPConnection, SocketError> {
        match peer_id {
            Some(peerid) => match self.peers.get(&peerid) {
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

    fn connect_internal(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        match TCPConnection::connect(address) {
            Ok(connection) => self.commit_onnection(connection),
            Err(_) => Err(ConnectorError::CouldNotConnect)
        }
    }

    fn get_peer_id_for_address(&self, address: SocketAddr) -> PeerId {
        self.addresses.get(&address).unwrap().clone()
    }

    fn set_peer_id_on_received_message(&self, message: RawMessage, address: SocketAddr) -> RawMessage {
        message.apply_peer_id(self.get_peer_id_for_address(address))
    }

    fn commit_onnection(&mut self, connection: TCPConnection) -> Result<PeerId, ConnectorError> {
        let peerid = PeerId::new_random();
        let addr = connection.get_address();

        self.peers.insert(peerid, connection);
        self.addresses.insert(addr, peerid);

        Ok(peerid)
    }

    fn is_already_connected(&self, address: &SocketAddr) -> bool {
        self.peers.values().find(|x| x.get_address() == *address).is_some()
    }
}

struct TCPConnectionManager {
    peers: Arc<Mutex<TCPConnectionManagerPeers>>,
    inward_queue: VecDeque<Result<RawMessage, (PeerId, SocketError)>>
}

impl TCPConnectionManager {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(TCPConnectionManagerPeers::new())),
            inward_queue: VecDeque::new()
        }
    }

    pub fn get_peers(&self) -> Arc<Mutex<TCPConnectionManagerPeers>> {
        self.peers.clone()
    }

    pub fn connect(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        let mut peers = self.peers.lock().unwrap();
        peers.connect(address)
    }

    pub fn send_message(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        let peers = self.peers.lock().unwrap();
        peers.send_message(message, flags)
    }

    fn receive_from_all_connections(&mut self) {
        let peers = self.peers.lock().unwrap();
        self.inward_queue.extend(peers.receive_from_all_connections().into_iter())
    }
}

impl Transport for TCPConnectionManager {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.send_message(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.receive_from_all_connections();
        let inward_entry_mapper = |inward_entry: Result<RawMessage, (PeerId, SocketError)>| {
            inward_entry.map_err(|(peer_id, error)| {(Some(peer_id), error)})
        };

        match flags {
            OpFlag::Default => {
                if let Some(inward_entry) = self.inward_queue.pop_front() {
                    inward_entry_mapper(inward_entry)
                } else {
                    util::thread::wait_for_backoff_mut(query_thread_default_duration_backoff(), || {
                        self.receive_from_all_connections();
                        self.inward_queue.pop_front().map(inward_entry_mapper)
                    })
                }
            }
            OpFlag::NoWait => {
                self.inward_queue.pop_front().map_or(Err((None, SocketError::Timeout)), inward_entry_mapper)
            },
            opflag => Err((None, (SocketError::UnsupportedOpFlag(opflag))))
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

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
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

struct TCPConnectionListener {
    manager: Arc<Mutex<TCPConnectionManagerPeers>>,
    listener: net::TcpListener
}

impl TCPConnectionListener {
    pub fn bind(addr: SocketAddr, manager: Arc<Mutex<TCPConnectionManagerPeers>>) -> Result<Self, ConnectorError> {
        Ok(Self {
            manager: manager,
            listener: net::TcpListener::bind(addr)?
        })
    }

    pub fn main_loop(self, stop_semaphore: StoppedSemaphore) -> Result<(), ConnectorError> {
        self.listener.set_nonblocking(true).unwrap();
        let mut sleep_backoff = query_acceptor_thread_default_duration_backoff();
        loop { 
            loop {
                match self.listener.accept() {
                    Ok(incoming) => {
                        let mut manager = self.manager.lock().unwrap();
                        manager.accept_connection(incoming).unwrap();
                        sleep_backoff.reset();
                    },
                    Err(err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) || 
                                matches!(err.kind(), std::io::ErrorKind::TimedOut) => {
                        break Ok(());
                    },
                    Err(err) => break Err(err)
                };
            }.unwrap();

            if stop_semaphore.is_stopped() {
                break;
            }

            thread::sleep(sleep_backoff.step());
        };

        Ok(())
    }
}

pub struct TCPAcceptorTransport {
    manager: TCPConnectionManager,
    listener_thread: Option<thread::JoinHandle<Result<(), ConnectorError>>>,
    stop_semaphore: StoppedSemaphore
}

impl TCPAcceptorTransport {
    pub fn new() -> Self {
        let stop_semaphore = StoppedSemaphore::new();
        Self {
            manager: TCPConnectionManager::new(),
            listener_thread: None,
            stop_semaphore: stop_semaphore
        }
    }
}

impl Transport for TCPAcceptorTransport {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.manager.receive(flags)
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

impl AcceptorTransport for TCPAcceptorTransport {
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        if let TransportMethod::Network(addr) = target {
            let listener = TCPConnectionListener::bind(addr, self.manager.get_peers())?;
            let stop_semaphore = self.stop_semaphore.clone();
            self.listener_thread = Some(thread::spawn(move || {listener.main_loop(stop_semaphore)}));
            Ok(None)
        } else {
            Err(ConnectorError::InvalidTransportMethod)
        }
    }
}

impl Drop for TCPAcceptorTransport {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.stop_semaphore.stop();
        self.listener_thread.take().unwrap().join().unwrap();
    }
}
