use super::internal::*;
use core::socket::{SocketInternalError};
use core::transport::{NetworkAddress};
use core::stream;

use std::net;
use std::io;

const SOCKET_READ_TIMEOUT_MS: u64 = 16;

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

#[derive(Clone)]
pub struct StreamListenerBuilder {

}

impl StreamListenerBuilder {
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
pub struct StreamConnectionBuilder {}

impl StreamConnectionBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl NetworkStreamConnectionBuilder for StreamConnectionBuilder {
    type Stream = net::TcpStream;

    fn connect(&self, addr: NetworkAddress) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketInternalError> {
        let stream = net::TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }

    fn accept_connection(&self, (stream, _addr): (net::TcpStream, NetworkAddress)) -> Result<stream::ReadWriteStreamConnection<net::TcpStream>, SocketInternalError> {
        stream.set_nonblocking(true)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(SOCKET_READ_TIMEOUT_MS)))?;
        Ok(stream::ReadWriteStreamConnection::new(stream))
    }
}

pub type InitiatorTransport = NetworkInitiatorTransport<StreamConnectionBuilder>;
pub type ConnectionListener = NetworkConnectionListener<net::TcpListener, StreamConnectionBuilder>;
pub type AcceptorTransport = NetworkAcceptorTransport<net::TcpListener, StreamListenerBuilder, StreamConnectionBuilder>;