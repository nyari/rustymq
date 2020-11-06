use core::message::{PeerId, Message, RawMessage};
use core::util;
use core::util::thread::{StoppedSemaphore, Sleeper};
use core::util::time::{LinearDurationBackoff, DurationBackoffWithDebounce};
use core::transport::{Transport, InitiatorTransport, AcceptorTransport, TransportMethod};
use core::socket::{ConnectorError, SocketError, OpFlag, PeerIdentification};
use stream;

use std::collections::{HashMap, HashSet, VecDeque};
use std::net;
use std::net::{SocketAddr};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use std::io;
use std::io::{Read, Write};
use std::iter::{FromIterator};
use std::marker::{PhantomData};

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

pub trait NetworkStream: Read + Write + Send + Sync + Sized + 'static {}

pub trait NetworkListener: Send + Sync + Sized + 'static {
    type Stream : NetworkStream;

    fn bind<A: net::ToSocketAddrs>(addr: A) -> io::Result<Self>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn try_clone(&self) -> io::Result<Self>;
    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)>;
    fn incoming(&self) -> net::Incoming<'_>;
    fn set_ttl(&self, ttl: u32) -> io::Result<()>;
    fn ttl(&self) -> io::Result<u32>;
    fn take_error(&self) -> io::Result<Option<io::Error>>;
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
}

pub trait NetworkStreamConnectionBuilder: Send + Sync + Sized + Clone + 'static
{
    type Stream: NetworkStream;

    fn connect(&self, addr: SocketAddr) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError>;

    fn accept_connection(&self, sa: (Self::Stream, SocketAddr)) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError>;

    fn manager_connect(&self, addr: SocketAddr) -> Result<stream::ReadWriteStreamConnectionManager, SocketError> {
        stream::ReadWriteStreamConnectionManager::construct_from_worker_handle(self.connect(addr)?)
    }

    fn manager_accept_connection(&self, stream: Self::Stream, addr: SocketAddr) -> Result<stream::ReadWriteStreamConnectionManager, SocketError> {
        stream::ReadWriteStreamConnectionManager::construct_from_worker_handle(self.accept_connection((stream, addr))?)
    }
}

pub struct NetworkConnectionManagerPeers {
    peer_table: HashMap<PeerId, stream::ReadWriteStreamConnectionManager>,
    addresses: HashMap<SocketAddr, PeerId>,
    peers: HashMap<PeerId, SocketAddr>
}

impl NetworkConnectionManagerPeers {
    pub fn new() -> Self {
        Self {
            peer_table: HashMap::new(),
            addresses: HashMap::new(),
            peers: HashMap::new()
        }
    }

    pub fn connect<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        if !self.is_already_connected(&address) {
            self.connect_internal(builder, address)
        } else {
            Err(ConnectorError::AlreadyConnected)
        }
    }

    pub fn accept_connection<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, (stream, addr): (Stream, SocketAddr)) -> Result<PeerId, ConnectorError> {
        match builder.manager_accept_connection(stream, addr) {
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

    pub fn query_connected_peers(&self) -> HashSet<PeerId> {
        HashSet::from_iter(self.peer_table.keys().map(|x| {x.clone()}))
    }

    pub fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.peer_table.remove(&peer_id).ok_or(ConnectorError::UnknownPeer)?;
                self.addresses.remove(&self.peers.remove(&peer_id).unwrap()).unwrap();
                Ok(None)
            },
            PeerIdentification::TransportMethod(TransportMethod::Network(addr)) => {
                let peer_id = self.addresses.remove(&addr).ok_or(ConnectorError::UnknownPeer)?;
                self.peers.remove(&peer_id).unwrap();
                self.peer_table.remove(&peer_id).unwrap();
                Ok(Some(peer_id))
            },
            _ => Err(ConnectorError::UnknownPeer)
        }
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

    fn connect_internal<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        match builder.manager_connect(address) {
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

struct NetworkConnectionManager {
    peers: Arc<Mutex<NetworkConnectionManagerPeers>>,
    inward_queue: VecDeque<Result<RawMessage, (PeerId, SocketError)>>
}

impl NetworkConnectionManager {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(NetworkConnectionManagerPeers::new())),
            inward_queue: VecDeque::new()
        }
    }

    pub fn get_peers(&self) -> Arc<Mutex<NetworkConnectionManagerPeers>> {
        self.peers.clone()
    }

    pub fn connect<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: SocketAddr) -> Result<PeerId, ConnectorError> {
        let mut peers = self.peers.lock().unwrap();
        peers.connect(builder, address)
    }

    pub fn send_message(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        let peers = self.peers.lock().unwrap();
        peers.send_message(message, flags)
    }

    fn close_connection_internal(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError> {
        let mut peers = self.peers.lock().unwrap();
        peers.close_connection(peer_identification)
    }

    fn receive_from_all_connections(&mut self) {
        let peers = self.peers.lock().unwrap();
        self.inward_queue.extend(peers.receive_from_all_connections().into_iter())
    }
}

impl Transport for NetworkConnectionManager {
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

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        let peers = self.peers.lock().unwrap();
        peers.query_connected_peers()
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError> {
        self.close_connection_internal(peer_identification)
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

pub struct NetworkInitiatorTransport<Builder: NetworkStreamConnectionBuilder> {
    manager: NetworkConnectionManager,
    builder: Builder
}

impl<Builder: NetworkStreamConnectionBuilder> NetworkInitiatorTransport<Builder> {
    pub fn new(builder: Builder) -> Self {
        Self {
            manager: NetworkConnectionManager::new(),
            builder: builder
        }
    }
}

impl<Builder: NetworkStreamConnectionBuilder> Transport for NetworkInitiatorTransport<Builder> {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.manager.receive(flags)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError> {
        self.manager.close_connection(peer_identification)
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        self.manager.query_connected_peers()
    }

    fn close(self) -> Result<(), SocketError> {
        self.manager.close()
    }
}

impl<Builder: NetworkStreamConnectionBuilder> InitiatorTransport for NetworkInitiatorTransport<Builder> {
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        match target {
            TransportMethod::Network(address) => Ok(Some(self.manager.connect(self.builder.clone(), address)?)),
            _ => Err(ConnectorError::InvalidTransportMethod)
        }
    }
}

pub struct NetworkConnectionListener<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> {
    manager: Arc<Mutex<NetworkConnectionManagerPeers>>,
    listener: Listener,
    builder: Builder
}

impl<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> NetworkConnectionListener<Listener, Builder> {
    pub fn bind(builder: Builder, addr: SocketAddr, manager: Arc<Mutex<NetworkConnectionManagerPeers>>) -> Result<Self, ConnectorError> {
        Ok(Self {
            manager: manager,
            listener: Listener::bind(addr)?,
            builder: builder
        })
    }

    pub fn main_loop(self, stop_semaphore: StoppedSemaphore) -> Result<(), ConnectorError> {
        self.listener.set_nonblocking(true).unwrap();
        let mut sleeper = Sleeper::new(query_acceptor_thread_default_duration_backoff());
        loop { 
            loop {
                match self.listener.accept() {
                    Ok(incoming) => {
                        let mut manager = self.manager.lock().unwrap();
                        manager.accept_connection(self.builder.clone(), incoming).unwrap();
                        sleeper.reset();
                    },
                    Err(err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) || 
                                matches!(err.kind(), std::io::ErrorKind::TimedOut) => {
                        break Ok(());
                    },
                    Err(err) => break Err(err)
                };
            }?;

            if stop_semaphore.is_stopped() {
                break;
            }

            sleeper.sleep();
        };

        Ok(())
    }
}

pub struct NetworkAcceptorTransport<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> {
    manager: NetworkConnectionManager,
    listener_thread: Option<thread::JoinHandle<Result<(), ConnectorError>>>,
    stop_semaphore: StoppedSemaphore,
    _listener: PhantomData<Listener>,
    builder: Builder
}

impl<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> NetworkAcceptorTransport<Listener, Builder> {
    pub fn new(builder: Builder) -> Self {
        let stop_semaphore = StoppedSemaphore::new();
        Self {
            manager: NetworkConnectionManager::new(),
            listener_thread: None,
            stop_semaphore: stop_semaphore,
            _listener: PhantomData,
            builder: builder
        }
    }
}

impl<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> Transport for NetworkAcceptorTransport<Listener, Builder> {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.manager.receive(flags)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError> {
        self.manager.close_connection(peer_identification)
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        self.manager.query_connected_peers()
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

impl<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> AcceptorTransport for NetworkAcceptorTransport<Listener, Builder> {
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        if let TransportMethod::Network(addr) = target {
            let listener: NetworkConnectionListener<Listener, Builder> = NetworkConnectionListener::bind(self.builder.clone(), addr, self.manager.get_peers())?;
            let stop_semaphore = self.stop_semaphore.clone();
            self.listener_thread = Some(thread::spawn(move || {listener.main_loop(stop_semaphore)}));
            Ok(None)
        } else {
            Err(ConnectorError::InvalidTransportMethod)
        }
    }
}

impl<Listener: NetworkListener, Builder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> Drop for NetworkAcceptorTransport<Listener, Builder> {
    fn drop(&mut self) {
        self.stop_semaphore.stop();
        self.listener_thread.take().and_then(|join_handle| {Some(join_handle.join())});
    }
}