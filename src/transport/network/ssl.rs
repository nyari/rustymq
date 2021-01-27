use super::internal::*;

use openssl::ssl::{SslConnector, SslStream, SslAcceptor};
use std::net::{SocketAddr};

use core::socket::{SocketInternalError};
use core::transport::{NetworkAddress};

use std::sync::Arc;

use std::net;
use std::io;
use stream;

const SOCKET_READ_TIMEOUT_MS: u64 = 16;

impl NetworkStream for SslStream<net::TcpStream> {}

pub struct StreamListener {
    socket: net::TcpListener,
    acceptor: SslAcceptor
}

impl StreamListener {
    pub fn new(socket: net::TcpListener, acceptor: SslAcceptor) -> Self {
        Self {
            socket: socket,
            acceptor: acceptor
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
            Err(_) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, "SSL Handshake failed"))
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
pub struct StreamListenerBuilder {
    acceptor_builder: Arc<dyn Fn() -> SslAcceptor + Sync + Send>
}

impl StreamListenerBuilder {
    pub fn new(acceptor_builder: Arc<dyn Fn() -> SslAcceptor + Sync + Send >) -> Self {
        Self {
            acceptor_builder: acceptor_builder
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
pub struct StreamConnectionBuilder {
    connector: SslConnector
}

impl StreamConnectionBuilder {
    pub fn new(connector: SslConnector) -> Self {
        Self {
            connector: connector
        }
    }
}

impl NetworkStreamConnectionBuilder for StreamConnectionBuilder {
    type Stream = SslStream<net::TcpStream>;

    fn connect(&self, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError> {
        let dns_address = addr.get_dns_name().ok_or(SocketInternalError::MissingDNSDomain)?;
        let stream = net::TcpStream::connect(addr.get_address())?;

        match self.connector.connect(dns_address.as_str(), stream) {
            Ok(mut ssl_stream) => { 
                let stream_mut_ref = ssl_stream.get_mut();
                stream_mut_ref.set_nonblocking(true)?;
                stream_mut_ref.set_write_timeout(None)?;
                stream_mut_ref.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
                Ok(stream::ReadWriteStreamConnection::new(ssl_stream))
            }
            Err(_) => {
                Err(SocketInternalError::HandshakeFailed)
            }
        }
    }

    fn accept_connection(&self, (mut stream, _addr): (Self::Stream, NetworkAddress)) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketInternalError> {
        let stream_mut_ref = stream.get_mut();
        stream_mut_ref.set_nonblocking(true)?;
        stream_mut_ref.set_write_timeout(None)?;
        stream_mut_ref.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }
}

pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
pub type ConnectionListener = NetworkConnectionListener<StreamListener, StreamConnectionBuilder>;
pub type AcceptorTransport = NetworkAcceptorTransport<StreamListener, StreamListenerBuilder, StreamConnectionBuilder>;