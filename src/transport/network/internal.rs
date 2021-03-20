use core::message::{PeerId, Message, RawMessage};
use core::util;
use core::util::thread::{StoppedSemaphore, Sleeper};
use core::util::time::{LinearDurationBackoff, DurationBackoffWithDebounce};
use core::transport::{Transport, InitiatorTransport, AcceptorTransport, TransportMethod, NetworkAddress};
use core::socket::{SocketError, SocketInternalError, OpFlag, PeerIdentification};
use core::stream;

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

    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn try_clone(&self) -> io::Result<Self>;
    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)>;
    fn set_ttl(&self, ttl: u32) -> io::Result<()>;
    fn ttl(&self) -> io::Result<u32>;
    fn take_error(&self) -> io::Result<Option<io::Error>>;
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
}

pub trait NetworkListenerBuilder: Send + Sync + Sized + Clone + 'static {
    type Listener : NetworkListener;
    fn bind<A: net::ToSocketAddrs>(&self, addr: A) -> io::Result<Self::Listener>;
}

pub trait NetworkStreamConnectionBuilder: Send + Sync + Sized + Clone + 'static
{
    type Stream: NetworkStream;

    fn connect(&self, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError>;

    fn accept_connection(&self, sa: (Self::Stream, NetworkAddress)) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError>;

    fn manager_connect(&self, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnectionManager, SocketInternalError> {
        stream::ReadWriteStreamConnectionManager::construct_from_worker_handle(self.connect(addr)?)
    }

    fn manager_accept_connection(&self, stream: Self::Stream, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnectionManager, SocketInternalError> {
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

    pub fn connect<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: NetworkAddress) -> Result<PeerId, SocketInternalError> {
        if !self.is_already_connected(&address.get_address()) {
            self.connect_internal(builder, address)
        } else {
            Err(SocketInternalError::AlreadyConnected)
        }
    }

    pub fn accept_connection<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, (stream, addr): (Stream, NetworkAddress)) -> Result<PeerId, SocketInternalError> {
        match builder.manager_accept_connection(stream, addr.clone()) {
            Ok(connection) => self.commit_onnection(connection, addr.get_address()),
            Err(_) => Err(SocketInternalError::CouldNotConnect)
        }
    }

    pub fn send_message(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketInternalError> {
        let connection = self.get_message_peer_connection(message.peer_id())?;
        match flags {
            OpFlag::Default => connection.send(message),
            OpFlag::NoWait => connection.send_async(message)
        }
    }

    pub fn receive_from_all_connections(&self) -> Vec<Result<RawMessage, (PeerId, SocketInternalError)>> {
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

    pub fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, SocketInternalError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.peer_table.remove(&peer_id).ok_or(SocketInternalError::UnknownPeer)?;
                self.addresses.remove(&self.peers.remove(&peer_id).unwrap()).unwrap();
                Ok(None)
            },
            PeerIdentification::TransportMethod(TransportMethod::Network(addr)) => {
                let peer_id = self.addresses.remove(&addr.get_address()).ok_or(SocketInternalError::UnknownPeer)?;
                self.peers.remove(&peer_id).unwrap();
                self.peer_table.remove(&peer_id).unwrap();
                Ok(Some(peer_id))
            },
            _ => Err(SocketInternalError::UnknownPeer)
        }
    }

    fn get_message_peer_connection<'a>(&'a self, peer_id: &Option<PeerId>) -> Result<&'a stream::ReadWriteStreamConnectionManager, SocketInternalError> {
        match peer_id {
            Some(peerid) => match self.peer_table.get(&peerid) {
                Some(connection) => Ok(connection),
                None => Err(SocketInternalError::UnknownPeer)
            },
            None => if self.peer_table.len() == 1 {
                Ok(self.peer_table.values().next().unwrap())
            } else {
                Err(SocketInternalError::UnknownPeer)
            }
        }
    }

    fn connect_internal<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: NetworkAddress) -> Result<PeerId, SocketInternalError> {
        match builder.manager_connect(address.clone()) {
            Ok(connection) => self.commit_onnection(connection, address.get_address()),
            Err(_) => Err(SocketInternalError::CouldNotConnect)
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

    fn commit_onnection(&mut self, connection: stream::ReadWriteStreamConnectionManager, addr: SocketAddr) -> Result<PeerId, SocketInternalError> {
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
    inward_queue: VecDeque<Result<RawMessage, (PeerId, SocketInternalError)>>
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

    pub fn connect<Stream: NetworkStream, Builder: NetworkStreamConnectionBuilder<Stream=Stream>>(&mut self, builder: Builder, address: NetworkAddress) -> Result<PeerId, SocketInternalError> {
        let mut peers = self.peers.lock().unwrap();
        peers.connect(builder, address)
    }

    pub fn send_message(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketInternalError> {
        let peers = self.peers.lock().unwrap();
        peers.send_message(message, flags)
    }

    fn close_connection_internal(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, SocketInternalError> {
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
        SocketInternalError::externalize_result(self.send_message(message, flags))
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.receive_from_all_connections();
        let inward_entry_mapper = |inward_entry: Result<RawMessage, (PeerId, SocketInternalError)>| {
            inward_entry.map_err(|(peer_id, error)| {(Some(peer_id), SocketError::from(error))})
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
            }
        }
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        let peers = self.peers.lock().unwrap();
        peers.query_connected_peers()
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, SocketError> {
        SocketInternalError::externalize_result(self.close_connection_internal(peer_identification))
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

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, SocketError> {
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
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        match target {
            TransportMethod::Network(address) => Ok(Some(self.manager.connect(self.builder.clone(), address)?)),
            _ => Err(SocketError::InvalidTransportMethod)
        }
    }
}

pub struct NetworkConnectionListener<Listener: NetworkListener, ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> {
    manager: Arc<Mutex<NetworkConnectionManagerPeers>>,
    listener: Listener,
    connection_builder: ConnectionBuilder
}

impl<Listener: NetworkListener, ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>> NetworkConnectionListener<Listener, ConnectionBuilder> {
    pub fn bind<ListenerBuilder: NetworkListenerBuilder<Listener=Listener>>(connection_builder: ConnectionBuilder, listener_builder: ListenerBuilder, addr: NetworkAddress, manager: Arc<Mutex<NetworkConnectionManagerPeers>>) -> Result<Self, SocketInternalError> {
        Ok(Self {
            manager: manager,
            listener: listener_builder.bind(addr)?,
            connection_builder: connection_builder
        })
    }

    pub fn main_loop(self, stop_semaphore: StoppedSemaphore) -> Result<(), SocketInternalError> {
        let mut sleeper = Sleeper::new(query_acceptor_thread_default_duration_backoff());
        loop { 
            loop {
                match self.listener.accept() {
                    Ok((stream, incoming_addr)) => {
                        let mut manager = self.manager.lock().unwrap();
                        manager.accept_connection(self.connection_builder.clone(), (stream, NetworkAddress::from_socket_addr(incoming_addr))).unwrap();
                        sleeper.reset();
                    },
                    Err(err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) => {
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

pub struct NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
    where Listener: NetworkListener,
          ListenerBuilder: NetworkListenerBuilder<Listener=Listener>,
          ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream> {
    manager: NetworkConnectionManager,
    listener_thread: Option<thread::JoinHandle<Result<(), SocketInternalError>>>,
    stop_semaphore: StoppedSemaphore,
    listener_builder: ListenerBuilder,
    connection_builder: ConnectionBuilder,
    _listener: PhantomData<Listener>
}

impl<Listener, ListenerBuilder, ConnectionBuilder> NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
    where Listener: NetworkListener,
          ListenerBuilder: NetworkListenerBuilder<Listener=Listener>,
          ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream> {
    pub fn new(connection_builder: ConnectionBuilder, listener_builder: ListenerBuilder) -> Self {
        let stop_semaphore = StoppedSemaphore::new();
        Self {
            manager: NetworkConnectionManager::new(),
            listener_thread: None,
            stop_semaphore: stop_semaphore,
            listener_builder: listener_builder,
            connection_builder: connection_builder,
            _listener: PhantomData
        }
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> Transport for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
    where Listener: NetworkListener,
          ListenerBuilder: NetworkListenerBuilder<Listener=Listener>,
          ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream> {

    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.manager.receive(flags)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, SocketError> {
        self.manager.close_connection(peer_identification)
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        self.manager.query_connected_peers()
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> AcceptorTransport for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder> 
    where Listener: NetworkListener,
          ListenerBuilder: NetworkListenerBuilder<Listener=Listener>,
          ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream>{
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        if let TransportMethod::Network(addr) = target {
            let listener_builder: NetworkConnectionListener<Listener, ConnectionBuilder> = NetworkConnectionListener::bind(self.connection_builder.clone(), self.listener_builder.clone(), addr, self.manager.get_peers())?;
            let stop_semaphore = self.stop_semaphore.clone();
            self.listener_thread = Some(thread::spawn(move || {listener_builder.main_loop(stop_semaphore)}));
            Ok(None)
        } else {
            Err(SocketError::InvalidTransportMethod)
        }
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> Drop for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder> 
    where Listener: NetworkListener,
          ListenerBuilder: NetworkListenerBuilder<Listener=Listener>,
          ConnectionBuilder: NetworkStreamConnectionBuilder<Stream=Listener::Stream> {

    fn drop(&mut self) {
        self.stop_semaphore.stop();
        self.listener_thread.take().and_then(|join_handle| {Some(join_handle.join())});
    }
}
