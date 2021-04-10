//! # Network transport core
//! Module containing the datastructures for specifiyng peers through network
use core::util::SingleIter;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::string::String;

/// #NetworkAddress
/// Contains the IP address and/or domain name of the peer to connect to
#[derive(Debug, Clone)]
pub struct NetworkAddress {
    address: SocketAddr,
    dns: Option<String>,
}

impl NetworkAddress {
    /// Create an instance with a socket address
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            address: addr,
            dns: None,
        }
    }

    /// Create an instance through a DNS name and port specification
    pub fn from_dns(dns: String) -> io::Result<Self> {
        for address in dns.to_socket_addrs()? {
            return Ok(Self {
                address: address,
                dns: Some(dns),
            });
        }
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "Did not find address for DNS",
        ))
    }

    /// Query the contained socket address
    pub fn get_address(&self) -> SocketAddr {
        self.address.clone()
    }

    /// Query the contained DNS name and port string if contained
    pub fn get_dns(&self) -> Option<String> {
        self.dns.clone()
    }

    /// Query the DNS name if contained
    pub fn get_dns_name(&self) -> Option<String> {
        self.dns
            .as_ref()
            .map(|dns| dns.split(":").next().unwrap().to_string())
    }
}

impl ToSocketAddrs for NetworkAddress {
    type Iter = SingleIter<SocketAddr>;
    fn to_socket_addrs<'a>(&'a self) -> io::Result<Self::Iter> {
        Ok(SingleIter::new(self.get_address()))
    }
}
