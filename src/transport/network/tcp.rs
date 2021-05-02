//! # Network transport through TCP
//! This module contains the [`crate::core::transport::Transport`] definitions to be able to use TCP based communication

use super::internal::*;
use core::config::TransportConfiguration;
use core::message::{PeerId, RawMessage};
use core::queue::{MessageQueueReceiver, MessageQueueSender};
use core::socket::SocketInternalError;
use core::stream;
use core::transport::NetworkAddress;

use std::io;
use std::net;

const SOCKET_READ_TIMEOUT_MS: u64 = 1;

impl NetworkStream for net::TcpStream {}

impl NetworkListener for net::TcpListener {
    type Stream = net::TcpStream;

    #[inline]
    fn local_addr(&self) -> io::Result<net::SocketAddr> {
        self.local_addr()
    }

    #[inline]
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }

    #[inline]
    fn accept(&self) -> io::Result<(Self::Stream, net::SocketAddr)> {
        self.accept()
    }

    #[inline]
    fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.set_ttl(ttl)
    }

    #[inline]
    fn ttl(&self) -> io::Result<u32> {
        self.ttl()
    }

    #[inline]
    fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.take_error()
    }

    #[inline]
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }
}

/// # TCP Stream listener builder
/// This structure is needed for constructing [`AcceptorTransport`]s to configure the underlieing TCP Listener
#[derive(Clone)]
pub struct StreamListenerBuilder {}

impl StreamListenerBuilder {
    /// Construct the stream listener builder
    pub fn new() -> Self {
        Self {}
    }
}

impl NetworkListenerBuilder for StreamListenerBuilder {
    type Listener = net::TcpListener;
    fn bind<A: net::ToSocketAddrs>(&self, addr: A) -> io::Result<Self::Listener> {
        let listener = net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true).unwrap();
        Ok(listener)
    }
}

#[derive(Clone)]
/// # TCP Stream connection builder
/// This structure is needed for constructing [`AcceptorTransport`]s and [`InitiatorTransport`] to configure the unerlieing TCP stream
pub struct StreamConnectionBuilder {}

impl StreamConnectionBuilder {
    /// Construct the stream connector builder
    pub fn new() -> Self {
        Self {}
    }
}

impl NetworkStreamConnectionBuilder for StreamConnectionBuilder {
    type Stream = net::TcpStream;

    fn connect(
        &self,
        config: &TransportConfiguration,
        addr: NetworkAddress,
        peer_id: PeerId,
        inward_queue: MessageQueueSender<RawMessage>,
    ) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketInternalError> {
        let stream = net::TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        //stream.set_nonblocking(true)?;
        stream.set_write_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(
            SOCKET_READ_TIMEOUT_MS,
        )))?;
        Ok(stream::ReadWriteStreamConnection::new(
            stream,
            MessageQueueReceiver::new(config.queue_policy.clone()),
            inward_queue,
            peer_id,
        ))
    }

    fn accept_connection(
        &self,
        config: &TransportConfiguration,
        stream: net::TcpStream,
        _addr: NetworkAddress,
        peer_id: PeerId,
        inward_queue: MessageQueueSender<RawMessage>,
    ) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketInternalError> {
        stream.set_nodelay(true)?;
        //stream.set_nonblocking(true)?;
        stream.set_write_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(
            SOCKET_READ_TIMEOUT_MS,
        )))?;
        Ok(stream::ReadWriteStreamConnection::new(
            stream,
            MessageQueueReceiver::new(config.queue_policy.clone()),
            inward_queue,
            peer_id,
        ))
    }
}

/// TCP Initiator transport to use with TCP  based connections
pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
/// TCP Acceptor transport to use with TCP  based connections
pub type AcceptorTransport =
    NetworkAcceptorTransport<net::TcpListener, StreamListenerBuilder, StreamConnectionBuilder>;
