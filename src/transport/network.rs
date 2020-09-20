use core::message::{PeerId, Message, RawMessage};
use core::util;
use core::util::thread::{StoppedSemaphore};
use core::util::time::{LinearDurationBackoff, DurationBackoff, DurationBackoffWithDebounce};
use core::transport::{Transport, InitiatorTransport, AcceptorTransport, TransportMethod};
use core::socket::{ConnectorError, SocketError, OpFlag};
use stream;

use std::collections::{HashMap, VecDeque};
use std::net;
use std::net::{SocketAddr};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{Duration};

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

struct TCPStreamConnectionBuilder {}

impl TCPStreamConnectionBuilder {
    pub fn connect(addr: SocketAddr) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketError> {
        let stream = net::TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }

    pub fn accept_connection((stream, _addr): (net::TcpStream, SocketAddr)) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketError> {
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }

    pub fn manager_connect(addr: SocketAddr) -> Result<stream::ReadWriteStreamConnectionManager, SocketError> {
        stream::ReadWriteStreamConnectionManager::construct_from_worker_handle(TCPStreamConnectionBuilder::connect(addr)?)
    }

    pub fn manager_accept_connection((stream, addr): (net::TcpStream, SocketAddr)) -> Result<stream::ReadWriteStreamConnectionManager, SocketError> {
        stream::ReadWriteStreamConnectionManager::construct_from_worker_handle(TCPStreamConnectionBuilder::accept_connection((stream, addr))?)
    }

}

struct TCPConnectionManagerPeers {
    peer_table: HashMap<PeerId, stream::ReadWriteStreamConnectionManager>,
    addresses: HashMap<SocketAddr, PeerId>,
    peers: HashMap<PeerId, SocketAddr>
}

impl TCPConnectionManagerPeers {
    pub fn new() -> Self {
        Self {
            peer_table: HashMap::new(),
            addresses: HashMap::new(),
            peers: HashMap::new()
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
        match TCPStreamConnectionBuilder::manager_accept_connection((stream, addr)) {
            Ok(connection) => self.commit_onnection(connection, addr),
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
        self.peer_table.iter().for_each(|(peer_id, connection)| {
            match connection.receive_async_all() {
                (messages, None) => {
                    results.extend(messages.into_iter().map(|message| { Ok(self.set_peer_id_on_received_message(message, &self.get_address_for_peer_id(peer_id))) }))
                },
                (messages, Some(err)) => {
                    if !messages.is_empty() {
                        results.extend(messages.into_iter().map(|message| { Ok(self.set_peer_id_on_received_message(message, &self.get_address_for_peer_id(peer_id))) }));
                        results.push(Err((peer_id.clone(), err)))
                    }
                }
            }
        });
        results
    }

    fn get_message_peer_connection<'a>(&'a self, peer_id: &Option<PeerId>) -> Result<&'a stream::ReadWriteStreamConnectionManager, SocketError> {
        match peer_id {
            Some(peerid) => match self.peer_table.get(&peerid) {
                Some(connection) => Ok(connection),
                None => Err(SocketError::UnknownPeer)
            },
            None => if self.peer_table.len() == 1 {
                Ok(self.peer_table.values().next().unwrap())
            } else {
                Err(SocketError::UnknownPeer)
            }
        }
    }

    fn connect_internal(&mut self, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        match TCPStreamConnectionBuilder::manager_connect(address) {
            Ok(connection) => self.commit_onnection(connection, address),
            Err(_) => Err(ConnectorError::CouldNotConnect)
        }
    }

    fn get_peer_id_for_address(&self, address: &SocketAddr) -> PeerId {
        self.addresses.get(address).unwrap().clone()
    }

    fn get_address_for_peer_id(&self, peer_id: &PeerId) -> SocketAddr {
        self.peers.get(peer_id).unwrap().clone()
    } 

    fn set_peer_id_on_received_message(&self, message: RawMessage, address: &SocketAddr) -> RawMessage {
        message.apply_peer_id(self.get_peer_id_for_address(address))
    }

    fn commit_onnection(&mut self, connection: stream::ReadWriteStreamConnectionManager, addr: SocketAddr) -> Result<PeerId, ConnectorError> {
        let peerid = PeerId::new_random();

        self.peer_table.insert(peerid, connection);
        self.addresses.insert(addr, peerid);
        self.peers.insert(peerid, addr);

        Ok(peerid)
    }

    fn is_already_connected(&self, address: &SocketAddr) -> bool {
        self.addresses.contains_key(address)
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
