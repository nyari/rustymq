use super::internal::*;

use openssl::ssl::{SslMethod, SslConnector, SslStream};
use std::net::TcpStream;

use core::socket::{SocketError};

use std::net;
use std::io;
use stream;

const SOCKET_READ_TIMEOUT_MS: u64 = 16;

impl NetworkStream for SslStream<net::TcpStream> {}

// impl NetworkListener for net::TcpListener {
//     type Stream = net::TcpStream;

//     #[inline]
//     fn bind<A: net::ToSocketAddrs>(addr: A) -> io::Result<Self> {
//         net::TcpListener::bind(addr)
//     }

//     #[inline]
//     fn local_addr(&self) -> io::Result<net::SocketAddr> {
//         self.local_addr()
//     }

//     #[inline]
//     fn try_clone(&self) -> io::Result<Self> {
//         self.try_clone()
//     }

//     #[inline]
//     fn accept(&self) -> io::Result<(Self::Stream, net::SocketAddr)> {
//         self.accept()
//     }

//     #[inline]
//     fn incoming(&self) -> net::Incoming<'_> {
//         self.incoming()
//     }

//     #[inline]
//     fn set_ttl(&self, ttl: u32) -> io::Result<()> {
//         self.set_ttl(ttl)
//     }

//     #[inline]
//     fn ttl(&self) -> io::Result<u32> {
//         self.ttl()
//     }

//     #[inline]
//     fn take_error(&self) -> io::Result<Option<io::Error>> {
//         self.take_error()
//     }

//     #[inline]
//     fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
//         self.set_nonblocking(nonblocking)
//     }
// }

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

    fn connect(&self, addr: net::SocketAddr) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError> {
        let stream = net::TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }

    fn accept_connection(&self, (stream, _addr): (Self::Stream, net::SocketAddr)) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError> {
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }
}

pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
//pub type ConnectionListener = NetworkConnectionListener<net::TcpListener, StreamConnectionBuilder>;
//pub type AcceptorTransport = NetworkAcceptorTransport<net::TcpListener, StreamConnectionBuilder>;