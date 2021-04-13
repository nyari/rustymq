//! # Internal helper module for network connections
//! This module contains functionality to establish stream based connectons through network

use core::config::TransportConfiguration;
use core::message::{Message, PeerId, RawMessage};
use core::queue::{InwardMessageQueueNotifier};
use core::socket::{OpFlag, PeerIdentification, SocketError, SocketInternalError};
use core::stream;
use core::transport::{
    AcceptorTransport, InitiatorTransport, NetworkAddress, Transport, TransportMethod,
};
use core::util::thread::{Semaphore, Sleeper};
use core::util::time::{DurationBackoffWithDebounce, LinearDurationBackoff};

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::io::{Read, Write};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::net;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn query_acceptor_thread_default_duration_backoff(
) -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(
        LinearDurationBackoff::new(Duration::from_millis(0), Duration::from_millis(100), 10),
        100,
    )
}

pub trait NetworkStream: Read + Write + Send + Sync + Sized + 'static {}

pub trait NetworkListener: Send + Sync + Sized + 'static {
    type Stream: NetworkStream;

    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn try_clone(&self) -> io::Result<Self>;
    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)>;
    fn set_ttl(&self, ttl: u32) -> io::Result<()>;
    fn ttl(&self) -> io::Result<u32>;
    fn take_error(&self) -> io::Result<Option<io::Error>>;
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
}

pub trait NetworkListenerBuilder: Send + Sync + Sized + Clone + 'static {
    type Listener: NetworkListener;
    fn bind<A: net::ToSocketAddrs>(&self, addr: A) -> io::Result<Self::Listener>;
}

pub trait NetworkStreamConnectionBuilder: Send + Sync + Sized + Clone + 'static {
    type Stream: NetworkStream;

    fn connect(
        &self,
        config: &TransportConfiguration,
        addr: NetworkAddress,
        inward_queue_notifier: InwardMessageQueueNotifier,
    ) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError>;

    fn accept_connection(
        &self,
        config: &TransportConfiguration,
        stream: Self::Stream,
        addr: NetworkAddress,
        inward_queue_notifier: InwardMessageQueueNotifier,
    ) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError>;

    fn manager_connect(
        &self,
        config: &TransportConfiguration,
        addr: NetworkAddress,
        inward_queue_notifier: InwardMessageQueueNotifier,
    ) -> Result<stream::ReadWriteStreamConnectionThreadManager, SocketInternalError> {
        stream::ReadWriteStreamConnectionThreadManager::execute_thread_for(self.connect(
            config,
            addr,
            inward_queue_notifier,
        )?)
    }

    fn manager_accept_connection(
        &self,
        config: &TransportConfiguration,
        stream: Self::Stream,
        addr: NetworkAddress,
        inward_queue_notifier: InwardMessageQueueNotifier,
    ) -> Result<stream::ReadWriteStreamConnectionThreadManager, SocketInternalError> {
        stream::ReadWriteStreamConnectionThreadManager::execute_thread_for(self.accept_connection(
            config,
            stream,
            addr,
            inward_queue_notifier,
        )?)
    }
}

pub struct NetworkConnectionPeerManager {
    peer_table: HashMap<PeerId, stream::ReadWriteStreamConnectionThreadManager>,
    addresses: HashMap<SocketAddr, PeerId>,
    peers: HashMap<PeerId, SocketAddr>,
    inward_queue_notifier: InwardMessageQueueNotifier,
    inward_queue: VecDeque<RawMessage>,
    config: TransportConfiguration,
}

impl NetworkConnectionPeerManager {
    pub fn new(config: TransportConfiguration) -> Self {
        Self {
            peer_table: HashMap::new(),
            addresses: HashMap::new(),
            peers: HashMap::new(),
            inward_queue_notifier: InwardMessageQueueNotifier::new(),
            inward_queue: VecDeque::new(),
            config: config,
        }
    }

    pub fn connect<
        Stream: NetworkStream,
        Builder: NetworkStreamConnectionBuilder<Stream = Stream>,
    >(
        &mut self,
        builder: Builder,
        address: NetworkAddress,
    ) -> Result<PeerId, SocketInternalError> {
        if !self.is_already_connected(&address.get_address()) {
            self.connect_internal(builder, address)
        } else {
            Err(SocketInternalError::AlreadyConnected)
        }
    }

    pub fn accept_connection<
        Stream: NetworkStream,
        Builder: NetworkStreamConnectionBuilder<Stream = Stream>,
    >(
        &mut self,
        builder: Builder,
        (stream, addr): (Stream, NetworkAddress),
    ) -> Result<PeerId, SocketInternalError> {
        let peer_id = PeerId::new_random();
        match builder.manager_accept_connection(
            &self.config,
            stream,
            addr.clone(),
            self.inward_queue_notifier.clone(),
        ) {
            Ok(connection) => {
                self.commit_onnection(connection, addr.get_address(), peer_id.clone());
                Ok(peer_id)
            }
            Err(_) => Err(SocketInternalError::CouldNotConnect),
        }
    }

    pub fn send_message(
        &mut self,
        message: RawMessage,
        flags: OpFlag,
    ) -> Result<(), SocketInternalError> {
        //self.handle_peer_error(message.peer_id().unwrap().clone())?;
        let connection = self.get_message_peer_connection(message.peer_id())?;
        match flags {
            OpFlag::Wait => connection.send(message),
            OpFlag::NoWait => connection.send_async(message),
        }
    }

    // fn handle_peer_error(&mut self, peer_id: PeerId) -> Result<(), SocketInternalError> {
    //     let (result, disconnect_peer) = match self.peer_table.get_mut(&peer_id) {
    //         Some(peer) => {
    //             (peer.check_worker_state(), true)
    //         },
    //         None => (Err(SocketInternalError::UnrelatedPeer), false)
    //     };

    //     println!("{:?}", result);

    //     if disconnect_peer {
    //         self.close_connection(PeerIdentification::PeerId(peer_id))?;
    //     }

    //     result
    // }

    fn handle_next_error(&mut self) -> Result<(), (Option<PeerId>, SocketInternalError)> {
        let lost_peer = self
            .peer_table
            .iter_mut()
            .find_map(|(peer_id, connection)| {
                if let Err(err) = connection.check_worker_state() {
                    Some((peer_id.clone(), err))
                } else {
                    None
                }
            });

        match lost_peer {
            Some((peer_id, err)) => {
                self.close_connection(PeerIdentification::PeerId(peer_id.clone()))
                    .unwrap();
                Err((Some(peer_id.clone()), err))
            }
            None => Ok(()),
        }
    }

    pub fn receive_message(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketInternalError)> {
        self.handle_next_error()?;
        match flags {
            OpFlag::Wait => { 
                self.receive_all_internal();
                Ok(self.inward_queue.pop_front().unwrap())
            },
            OpFlag::NoWait => {
                self.receive_async_all_internal();
                self.inward_queue.pop_front().ok_or((None, SocketInternalError::Timeout))
            }
        }
    }

    pub fn query_connected_peers(&self) -> HashSet<PeerId> {
        HashSet::from_iter(self.peer_table.keys().map(|x| x.clone()))
    }

    pub fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketInternalError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.peer_table
                    .remove(&peer_id)
                    .ok_or(SocketInternalError::UnknownPeer)?;
                self.addresses
                    .remove(&self.peers.remove(&peer_id).unwrap())
                    .unwrap();
                Ok(None)
            }
            PeerIdentification::TransportMethod(TransportMethod::Network(addr)) => {
                let peer_id = self
                    .addresses
                    .remove(&addr.get_address())
                    .ok_or(SocketInternalError::UnknownPeer)?;
                self.peers.remove(&peer_id).unwrap();
                self.peer_table.remove(&peer_id).unwrap();
                Ok(Some(peer_id))
            }
            _ => Err(SocketInternalError::UnknownPeer),
        }
    }

    fn get_message_peer_connection<'a>(
        &'a mut self,
        peer_id: &Option<PeerId>,
    ) -> Result<&'a mut stream::ReadWriteStreamConnectionThreadManager, SocketInternalError> {
        match peer_id {
            Some(peerid) => match self.peer_table.get_mut(&peerid) {
                Some(connection) => Ok(connection),
                None => Err(SocketInternalError::UnknownPeer),
            },
            None => {
                if self.peer_table.len() == 1 {
                    Ok(self.peer_table.values_mut().next().unwrap())
                } else {
                    Err(SocketInternalError::UnknownPeer)
                }
            }
        }
    }

    fn connect_internal<
        Stream: NetworkStream,
        Builder: NetworkStreamConnectionBuilder<Stream = Stream>,
    >(
        &mut self,
        builder: Builder,
        address: NetworkAddress,
    ) -> Result<PeerId, SocketInternalError> {
        let peer_id = PeerId::new_random();

        match builder.manager_connect(
            &self.config,
            address.clone(),
            self.inward_queue_notifier.clone(),
        ) {
            Ok(connection) => {
                Ok(self.commit_onnection(connection, address.get_address(), peer_id.clone()))
            }
            Err(_) => Err(SocketInternalError::CouldNotConnect),
        }?;

        Ok(peer_id)
    }

    fn commit_onnection(
        &mut self,
        connection: stream::ReadWriteStreamConnectionThreadManager,
        addr: SocketAddr,
        peer_id: PeerId,
    ) {
        self.peer_table.insert(peer_id, connection);
        self.addresses.insert(addr, peer_id);
        self.peers.insert(peer_id, addr);
    }

    fn is_already_connected(&self, address: &SocketAddr) -> bool {
        self.addresses.contains_key(address)
    }

    fn receive_all_internal(&mut self) {
        if self.inward_queue.is_empty() && self.inward_queue_notifier.wait_for_change() {
            self.receive_fill_internal_queue();
        }
    }

    fn receive_async_all_internal(&mut self) {
        if self.inward_queue.is_empty() && self.inward_queue_notifier.take_was_change() {
            self.receive_fill_internal_queue();
        }
    }

    fn receive_fill_internal_queue(&mut self) {
        for (peer_id, peer) in self.peer_table.iter_mut() {
            self.inward_queue.extend(peer.receive_async_all().into_iter().map(|message| {message.apply_peer_id(peer_id.clone())}))    
        };
    }
}

struct NetworkConnectionManager {
    peers: Arc<Mutex<NetworkConnectionPeerManager>>,
    config: TransportConfiguration,
}

impl NetworkConnectionManager {
    pub fn new(config: TransportConfiguration) -> Self {
        Self {
            peers: Arc::new(Mutex::new(NetworkConnectionPeerManager::new(
                config.clone(),
            ))),
            config
        }
    }

    pub fn get_peers(&self) -> Arc<Mutex<NetworkConnectionPeerManager>> {
        self.peers.clone()
    }

    pub fn connect<
        Stream: NetworkStream,
        Builder: NetworkStreamConnectionBuilder<Stream = Stream>,
    >(
        &mut self,
        builder: Builder,
        address: NetworkAddress,
    ) -> Result<PeerId, SocketInternalError> {
        let mut peers = self.peers.lock().unwrap();
        peers.connect(builder, address)
    }

    pub fn send_message(
        &mut self,
        message: RawMessage,
        flags: OpFlag,
    ) -> Result<(), SocketInternalError> {
        let mut peers = self.peers.lock().unwrap();
        peers.send_message(message, flags)
    }

    pub fn receive_message(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketInternalError)> {
        let mut peers = self.peers.lock().unwrap();
        peers.receive_message(flags)
    }

    fn close_connection_internal(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketInternalError> {
        let mut peers = self.peers.lock().unwrap();
        peers.close_connection(peer_identification)
    }
}

impl Transport for NetworkConnectionManager {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        SocketInternalError::externalize_result(self.send_message(message, flags))
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.receive_message(flags).map_err(|(peer, err)| (peer, SocketInternalError::externalize_error(err)))
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        let peers = self.peers.lock().unwrap();
        peers.query_connected_peers()
    }

    fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketError> {
        SocketInternalError::externalize_result(self.close_connection_internal(peer_identification))
    }

    fn query_configuration(&self) -> Option<&TransportConfiguration> {
        Some(&self.config)
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

pub struct NetworkInitiatorTransport<Builder: NetworkStreamConnectionBuilder> {
    manager: NetworkConnectionManager,
    builder: Builder,
    config: TransportConfiguration,
}

impl<Builder: NetworkStreamConnectionBuilder> NetworkInitiatorTransport<Builder> {
    pub fn new(builder: Builder) -> Self {
        let config = TransportConfiguration::new();
        Self {
            manager: NetworkConnectionManager::new(config.clone()),
            builder: builder,
            config: config,
        }
    }

    pub fn with_configuration(builder: Builder, config: TransportConfiguration) -> Self {
        Self {
            manager: NetworkConnectionManager::new(config.clone()),
            builder: builder,
            config: config,
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

    fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketError> {
        self.manager.close_connection(peer_identification)
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        self.manager.query_connected_peers()
    }

    fn query_configuration(&self) -> Option<&TransportConfiguration> {
        Some(&self.config)
    }

    fn close(self) -> Result<(), SocketError> {
        self.manager.close()
    }
}

impl<Builder: NetworkStreamConnectionBuilder> InitiatorTransport
    for NetworkInitiatorTransport<Builder>
{
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        match target {
            TransportMethod::Network(address) => {
                Ok(Some(self.manager.connect(self.builder.clone(), address)?))
            }
            _ => Err(SocketError::InvalidTransportMethod),
        }
    }
}

pub struct NetworkConnectionListener<
    Listener: NetworkListener,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
> {
    manager: Arc<Mutex<NetworkConnectionPeerManager>>,
    listener: Listener,
    connection_builder: ConnectionBuilder,
}

impl<
        Listener: NetworkListener,
        ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
    > NetworkConnectionListener<Listener, ConnectionBuilder>
{
    pub fn bind<ListenerBuilder: NetworkListenerBuilder<Listener = Listener>>(
        connection_builder: ConnectionBuilder,
        listener_builder: ListenerBuilder,
        addr: NetworkAddress,
        manager: Arc<Mutex<NetworkConnectionPeerManager>>,
    ) -> Result<Self, SocketInternalError> {
        Ok(Self {
            manager: manager,
            listener: listener_builder.bind(addr)?,
            connection_builder: connection_builder,
        })
    }

    pub fn main_loop(self, stop_semaphore: Semaphore) -> Result<(), SocketInternalError> {
        let mut sleeper = Sleeper::new(query_acceptor_thread_default_duration_backoff());
        loop {
            loop {
                match self.listener.accept() {
                    Ok((stream, incoming_addr)) => {
                        let mut manager = self.manager.lock().unwrap();
                        manager
                            .accept_connection(
                                self.connection_builder.clone(),
                                (stream, NetworkAddress::from_socket_addr(incoming_addr)),
                            )
                            .unwrap();
                        sleeper.reset();
                    }
                    Err(err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) => {
                        break Ok(());
                    }
                    Err(err) => break Err(err),
                };
            }?;

            if stop_semaphore.is_signaled() {
                break;
            }

            sleeper.sleep();
        }

        Ok(())
    }
}

pub struct NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
where
    Listener: NetworkListener,
    ListenerBuilder: NetworkListenerBuilder<Listener = Listener>,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
{
    manager: NetworkConnectionManager,
    listener_thread: Option<thread::JoinHandle<Result<(), SocketInternalError>>>,
    stop_semaphore: Semaphore,
    listener_builder: ListenerBuilder,
    connection_builder: ConnectionBuilder,
    config: TransportConfiguration,
    _listener: PhantomData<Listener>,
}

impl<Listener, ListenerBuilder, ConnectionBuilder>
    NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
where
    Listener: NetworkListener,
    ListenerBuilder: NetworkListenerBuilder<Listener = Listener>,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
{
    pub fn new(connection_builder: ConnectionBuilder, listener_builder: ListenerBuilder) -> Self {
        let stop_semaphore = Semaphore::new();
        Self {
            manager: NetworkConnectionManager::new(TransportConfiguration::new()),
            listener_thread: None,
            stop_semaphore: stop_semaphore,
            listener_builder: listener_builder,
            connection_builder: connection_builder,
            config: TransportConfiguration::new(),
            _listener: PhantomData,
        }
    }

    pub fn with_configuration(
        connection_builder: ConnectionBuilder,
        listener_builder: ListenerBuilder,
        config: TransportConfiguration,
    ) -> Self {
        let stop_semaphore = Semaphore::new();
        Self {
            manager: NetworkConnectionManager::new(config.clone()),
            listener_thread: None,
            stop_semaphore: stop_semaphore,
            listener_builder: listener_builder,
            connection_builder: connection_builder,
            config: config,
            _listener: PhantomData,
        }
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> Transport
    for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
where
    Listener: NetworkListener,
    ListenerBuilder: NetworkListenerBuilder<Listener = Listener>,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
{
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError> {
        self.manager.send(message, flags)
    }

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.manager.receive(flags)
    }

    fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketError> {
        self.manager.close_connection(peer_identification)
    }

    fn query_connected_peers(&self) -> HashSet<PeerId> {
        self.manager.query_connected_peers()
    }

    fn query_configuration(&self) -> Option<&TransportConfiguration> {
        Some(&self.config)
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> AcceptorTransport
    for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
where
    Listener: NetworkListener,
    ListenerBuilder: NetworkListenerBuilder<Listener = Listener>,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
{
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        if let TransportMethod::Network(addr) = target {
            let listener_builder: NetworkConnectionListener<Listener, ConnectionBuilder> =
                NetworkConnectionListener::bind(
                    self.connection_builder.clone(),
                    self.listener_builder.clone(),
                    addr,
                    self.manager.get_peers(),
                )?;
            let stop_semaphore = self.stop_semaphore.clone();
            self.listener_thread = Some(thread::spawn(move || {
                listener_builder.main_loop(stop_semaphore)
            }));
            Ok(None)
        } else {
            Err(SocketError::InvalidTransportMethod)
        }
    }
}

impl<Listener, ListenerBuilder, ConnectionBuilder> Drop
    for NetworkAcceptorTransport<Listener, ListenerBuilder, ConnectionBuilder>
where
    Listener: NetworkListener,
    ListenerBuilder: NetworkListenerBuilder<Listener = Listener>,
    ConnectionBuilder: NetworkStreamConnectionBuilder<Stream = Listener::Stream>,
{
    fn drop(&mut self) {
        self.stop_semaphore.signal();
        self.listener_thread
            .take()
            .and_then(|join_handle| Some(join_handle.join()));
    }
}
