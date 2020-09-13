use core::message::{RawMessage, PeerId};
use core::socket::{SocketError, ConnectorError, OpFlag};
use std::net::{SocketAddr};

pub enum TransportMethod
{
    Network(SocketAddr),
    FAIL
}

pub trait Transport: Send + Sync {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError>;
    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, SocketError>;
    fn close(self) -> Result<(), SocketError>;
}

pub trait InitiatorTransport : Transport {
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
}

pub trait AcceptorTransport : Transport {
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
}

pub trait BidirectionalTransport : InitiatorTransport + AcceptorTransport {
    
}