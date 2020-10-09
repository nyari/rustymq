use core::message::{RawMessage, PeerId};
use core::socket::{SocketError, ConnectorError, OpFlag, PeerIdentification};
use std::net::{SocketAddr};
use std::collections::{HashSet};

#[derive(Debug)]
pub enum TransportMethod
{
    Network(SocketAddr),
    Dummy
}

pub trait Transport: Send + Sync {
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError>;
    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)>;
    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<Option<PeerId>, ConnectorError>;
    fn query_connected_peers(&self) -> HashSet<PeerId>;
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