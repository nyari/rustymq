use std::net::{SocketAddr, ToSocketAddrs};
use std::io;
use std::string::{String};
use core::util::SingleIter;

#[derive(Debug)]
#[derive(Clone)]
pub struct NetworkAddress {
    address: SocketAddr,
    dns: Option<String>
}

impl NetworkAddress {
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            address: addr,
            dns: None
        }
    }

    pub fn from_dns(dns: String) -> io::Result<Self> {
        for address in dns.to_socket_addrs()? {
            return Ok(Self {
                address: address,
                dns: Some(dns)
            })
        }
        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "Did not find address for DNS"))
    }

    pub fn get_address(&self) -> SocketAddr {
        self.address.clone()
    }

    pub fn get_dns(&self) -> Option<String> {
        self.dns.clone()
    }

    pub fn get_dns_name(&self) -> Option<String> {
        self.dns.as_ref().map(|dns| {
            dns.split(":").next().unwrap().to_string()
        })
    }
}

impl ToSocketAddrs for NetworkAddress {
    type Iter = SingleIter<SocketAddr>;
    fn to_socket_addrs<'a>(&'a self) -> io::Result<Self::Iter> {
        Ok(SingleIter::new(self.get_address()))
    }
}