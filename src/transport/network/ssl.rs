//! # Network transport through SSL
//! This module contains the [`crate::base::Transport`] definitions to be able to use SSL based communication

use super::common::*;

use openssl::ssl::{SslAcceptor, SslConnector, SslStream};

use crate::base::config::TransportConfiguration;
use crate::base::message::{PeerId, RawMessage};
use crate::base::transport::NetworkAddress;
use crate::internals::queue::{MessageQueueReceiver, MessageQueueSender};
use crate::internals::socket::SocketInternalError;
use crate::internals::stream;

use std::sync::Arc;

use std::io;
use std::net;
use std::net::SocketAddr;

const SOCKET_READ_TIMEOUT_MS: u64 = 1;

impl NetworkStream for SslStream<net::TcpStream> {}

pub struct StreamListener {
    socket: net::TcpListener,
    acceptor: SslAcceptor,
}

impl StreamListener {
    pub fn new(socket: net::TcpListener, acceptor: SslAcceptor) -> Self {
        Self {
            socket: socket,
            acceptor: acceptor,
        }
    }
}

impl NetworkListener for StreamListener {
    type Stream = SslStream<net::TcpStream>;

    #[inline]
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    fn try_clone(&self) -> io::Result<Self> {
        Ok(Self::new(self.socket.try_clone()?, self.acceptor.clone()))
    }

    #[inline]
    fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
        let acceptor = self.acceptor.clone();
        let (stream, addr) = self.socket.accept()?;
        match acceptor.accept(stream) {
            Ok(ssl_stream) => Ok((ssl_stream, addr)),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "SSL Handshake failed",
            )),
        }
    }

    #[inline]
    fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_ttl(ttl)
    }

    #[inline]
    fn ttl(&self) -> io::Result<u32> {
        self.socket.ttl()
    }

    #[inline]
    fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.socket.take_error()
    }

    #[inline]
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }
}

#[derive(Clone)]
/// # SSL Stream listener builder
/// This structure is needed for constructing [`AcceptorTransport`]s to configure the underlieing SSL Listener
pub struct StreamListenerBuilder {
    acceptor_builder: Arc<dyn Fn() -> SslAcceptor + Sync + Send>,
}

impl StreamListenerBuilder {
    /// Construct a new listener builder that allows for listening to SLL connections
    /// The acceptor_builder is a factory function that can construct an `openssl::ssl::SslAcceptor`.
    /// For details please see OpenSSL crate
    pub fn new(acceptor_builder: Arc<dyn Fn() -> SslAcceptor + Sync + Send>) -> Self {
        Self {
            acceptor_builder: acceptor_builder,
        }
    }
}

impl NetworkListenerBuilder for StreamListenerBuilder {
    type Listener = StreamListener;
    fn bind<A: net::ToSocketAddrs>(&self, addr: A) -> io::Result<Self::Listener> {
        let listener = net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true).unwrap();
        let acceptor = (self.acceptor_builder)();
        Ok(StreamListener::new(listener, acceptor))
    }
}

#[derive(Clone)]
/// # SSL Stream connection builder
/// This structure is needed for constructing [`AcceptorTransport`]s and [`InitiatorTransport`] to configure the unerlieing SSL stream
pub struct StreamConnectionBuilder {
    connector: SslConnector,
}

impl StreamConnectionBuilder {
    /// Construct a new connector builder that allows for connecting to SLL listeners
    /// The connector parameter is an instance of an `openssl::ssl::SslConnector` that allows for creating SSL connections
    /// For details please see OpenSSL crate
    pub fn new(connector: SslConnector) -> Self {
        Self {
            connector: connector,
        }
    }
}

impl NetworkStreamConnectionBuilder for StreamConnectionBuilder {
    type Stream = SslStream<net::TcpStream>;

    fn connect(
        &self,
        config: &TransportConfiguration,
        addr: NetworkAddress,
        peer_id: PeerId,
        inward_queue: MessageQueueSender<RawMessage>,
    ) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError> {
        let dns_address = addr
            .get_dns_name()
            .ok_or(SocketInternalError::MissingDNSDomain)?;
        let stream = net::TcpStream::connect(addr.get_address())?;

        match self.connector.connect(dns_address.as_str(), stream) {
            Ok(mut ssl_stream) => {
                let stream_mut_ref = ssl_stream.get_mut();
                stream_mut_ref.set_write_timeout(Some(std::time::Duration::from_millis(
                    SOCKET_READ_TIMEOUT_MS,
                )))?;
                stream_mut_ref.set_read_timeout(Some(std::time::Duration::from_millis(
                    SOCKET_READ_TIMEOUT_MS,
                )))?;
                Ok(stream::ReadWriteStreamConnection::new(
                    ssl_stream,
                    MessageQueueReceiver::new(config.queue_policy.clone()),
                    inward_queue,
                    peer_id,
                ))
            }
            Err(_) => Err(SocketInternalError::HandshakeFailed),
        }
    }

    fn accept_connection(
        &self,
        config: &TransportConfiguration,
        mut stream: Self::Stream,
        _addr: NetworkAddress,
        peer_id: PeerId,
        inward_queue: MessageQueueSender<RawMessage>,
    ) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError> {
        let stream_mut_ref = stream.get_mut();
        stream_mut_ref.set_write_timeout(Some(std::time::Duration::from_millis(
            SOCKET_READ_TIMEOUT_MS,
        )))?;
        stream_mut_ref.set_read_timeout(Some(std::time::Duration::from_millis(
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

/// TCP Initiator transport to use with SSL  based connections
pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
/// TCP Acceptor transport to use with SSL  based connections
pub type AcceptorTransport =
    NetworkAcceptorTransport<StreamListener, StreamListenerBuilder, StreamConnectionBuilder>;
