use super::internal::*;

use openssl::ssl::{SslMethod, SslConnector, SslStream, HandshakeError, SslAcceptorBuilder};
use std::net::TcpStream;

use core::socket::{SocketError};
use core::transport::{NetworkAddress};

use std::net;
use std::io;
use stream;

const SOCKET_READ_TIMEOUT_MS: u64 = 16;

impl NetworkStream for SslStream<net::TcpStream> {}

struct SslListener {
    acceptor_builder: SslAcceptorBuilder
}

impl NetworkListener for SslListeter {

}
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

    fn connect(&self, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError> {
        let dns_address = addr.get_dns().ok_or(SocketError::MissingDNSDomain)?;
        let stream = net::TcpStream::connect(addr.get_address())?;

        match self.connector.connect(dns_address.as_str(), stream) {
            Ok(mut ssl_stream) => { 
                let stream_mut_ref = ssl_stream.get_mut();
                stream_mut_ref.set_nonblocking(true)?;
                stream_mut_ref.set_write_timeout(None)?;
                stream_mut_ref.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
                Ok(stream::ReadWriteStreamConnection::new(ssl_stream))
            }
            Err(_) => Err(SocketError::HandshakeFailed)
        }
    }

    fn accept_connection(&self, (stream, _addr): (Self::Stream, NetworkAddress)) -> Result<stream::ReadWriteStreamConnection<Self::Stream>, SocketError> {
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }
}

pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
//pub type ConnectionListener = NetworkConnectionListener<net::TcpListener, StreamConnectionBuilder>;
//pub type AcceptorTransport = NetworkAcceptorTransport<net::TcpListener, StreamConnectionBuilder>;