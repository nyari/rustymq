use std::str::{FromStr};
use regex::{Regex};

#[derive(Debug)]
#[derive(PartialEq)]
pub enum SubnetParsingError
{
    IncorrectStringFormat,
    SubnetOutOfRange
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum AddressParsingError
{
    AddressGroupOutOfBounds,
    IncorrectStringFormat,
    Subnet(SubnetParsingError)
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum SubnetError
{
    MaximumBitsError(usize)
}

pub trait IPAddress 
{
    fn bit_depth() -> usize;
}

pub trait TransportProtocol
{
    fn get_port(&self) -> u16;
}

#[derive(Debug)]
pub struct IPv4Subnet
{
    bits: usize
}

#[derive(Debug)]
pub struct IPv4Address
{
    value: [u8; 4],
    subnet: Option<IPv4Subnet>
}

#[derive(Debug)]
pub struct IPv6Subnet
{
    bits: usize
}

#[derive(Debug)]
pub struct IPv6Address
{
    value: [u16; 8],
    subnet: Option<IPv6Subnet> 
}

#[derive(Debug)]
pub struct TCPTransport
{
    port: u16
}

#[derive(Debug)]
pub struct UDPTransport
{
    port: u16
}

#[derive(Debug)]
pub enum Address 
{
    IPv4(IPv4Address),
    IPv6(IPv6Address)
}

#[derive(Debug)]
pub enum Subnet
{
    IPv4(IPv4Subnet),
    IPv6(IPv6Subnet)
}

#[derive(Debug)]
pub enum Transport
{
    TCP(TCPTransport),
    UDP(UDPTransport)
}

#[derive(Debug)]
pub struct NetworkAddress
{
    address: Option<Address>,
    transport: Option<Transport>
}


impl IPv4Subnet
{
    pub fn new_bits(bits:usize) -> Result<Self, SubnetError> {
        if bits <= 32 {
            Ok(Self {
                bits:bits
            })
        } else {
            Err(SubnetError::MaximumBitsError(32))
        }
    }
}


impl FromStr for IPv4Subnet {
    type Err = SubnetParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<usize>() {
            Ok(value) => {
                match Self::new_bits(value) {
                    Ok(result) => Ok(result),
                    Err(SubnetError::MaximumBitsError(_)) => Err(SubnetParsingError::SubnetOutOfRange)
                }
            },
            Err(_) => Err(SubnetParsingError::IncorrectStringFormat)
        }
    }
}


impl IPv4Address 
{
    pub fn new_address(n1:u8, n2:u8, n3:u8, n4:u8) -> Self {
        let result_value:[u8;4] = [n1, n2, n3, n4];
        Self {
            value:result_value,
            subnet:None
        }
    }

    pub fn new_address_from_array(address:[u8; 4]) -> Self {
        Self {
            value:address,
            subnet:None
        }
    }

    pub fn new_address_from_slice(address:&[u8]) -> Result<Self, AddressParsingError> {
        if address.len() == 4 {
            Ok(Self::new_address_from_array([address[0], address[1], address[2], address[3]]))
        } else {
            Err(AddressParsingError::AddressGroupOutOfBounds)
        }
    } 

    pub fn new_subnet(n1:u8, n2:u8, n3:u8, n4:u8, subnet: IPv4Subnet) -> Self {
        let address = Self::new_address(n1, n2, n3, n4);
        Self {
            subnet:Some(subnet),
            ..address
        }
    }

    pub fn new_subnet_from_array(address:[u8; 4], subnet: IPv4Subnet) -> Self {
        let address = Self::new_address_from_array(address);
        Self {
            subnet:Some(subnet),
            ..address
        }
    }

    pub fn new_subnet_from_slice(address:&[u8], subnet: IPv4Subnet) -> Result<Self, AddressParsingError> {
        if address.len() == 4 {
            Ok(Self::new_subnet_from_array([address[0], address[1], address[2], address[3]], subnet))
        } else {
            Err(AddressParsingError::AddressGroupOutOfBounds)
        }
    } 

}


impl FromStr for IPv4Address
{
    type Err = AddressParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let test_regex = Regex::new(r"^([\w\d:/]{4,})?(?P<a>\d{1,3})\.(?P<b>\d{1,3})\.(?P<c>\d{1,3})\.(?P<d>\d{1,3})(/(?P<sub>[0-9\.]+))?$").unwrap();
        match test_regex.captures(s) {
            Some(captures) => {
                let values : Vec<u8> = ["a", "b", "c", "d"].into_iter()
                                                           .map(|x| captures.name(x).unwrap().as_str().parse::<u8>())
                                                           .filter(|x| x.is_ok())
                                                           .map(|x| x.unwrap())
                                                           .collect();
                
                match captures.name("sub") {
                    Some(value) => {
                        match value.as_str().parse::<IPv4Subnet>() {
                            Ok(subnet) => Self::new_subnet_from_slice(values.as_slice(), subnet),
                            Err(err) => Err(AddressParsingError::Subnet(err))
                        }
                    },
                    None => Self::new_address_from_slice(values.as_slice())
                }
            },
            None => Err(AddressParsingError::IncorrectStringFormat)
        }

    }
}



#[cfg(test)]
mod tests
{
    use super::*;

    #[test]
    fn ipv4subnet_new_bits_correct() {
        let subnet = IPv4Subnet::new_bits(16);
        assert_eq!(16, subnet.unwrap().bits);
    }

    #[test]
    fn ipv4subnet_new_bits_errored() {
        match IPv4Subnet::new_bits(42) {
            Err(SubnetError::MaximumBitsError(32)) => (),
            _ => panic!()
        }
    }

    #[test]
    fn ipv4address_new_address() {
        let new_address = IPv4Address::new_address(192,168,0,1);
        assert_eq!(new_address.value, [192,168,0,1]);
        assert!(new_address.subnet.is_none());
    }

        #[test]
    fn ipv4address_new_address_from_array() {
        let new_address = IPv4Address::new_address_from_array([192,168,0,1]);
        assert_eq!(new_address.value, [192,168,0,1]);
        assert!(new_address.subnet.is_none());
    }

    #[test]
    fn ipv4address_new_subnet() {
        let new_address = IPv4Address::new_subnet(192,168,0,1,IPv4Subnet::new_bits(24).unwrap());
        assert_eq!(new_address.value, [192,168,0,1]);
        assert_eq!(new_address.subnet.unwrap().bits, 24);
    }

    #[test]
    fn ipv4address_new_subnet_from_array() {
        let new_address = IPv4Address::new_subnet_from_array([192,168,0,1], IPv4Subnet::new_bits(24).unwrap());
        assert_eq!(new_address.value, [192,168,0,1]);
        assert_eq!(new_address.subnet.unwrap().bits, 24);
    }

    #[test]
    fn ipv4subnet_parse_from_string() {
        assert_eq!("24".parse::<IPv4Subnet>().unwrap().bits, 24);
        assert_eq!("32".parse::<IPv4Subnet>().unwrap().bits, 32);
        assert_eq!("34".parse::<IPv4Subnet>().unwrap_err(), SubnetParsingError::SubnetOutOfRange);
        assert_eq!("nfks".parse::<IPv4Subnet>().unwrap_err(), SubnetParsingError::IncorrectStringFormat);
    }

    #[test]
    fn ipv4address_parse_from_string() {
        assert!("192.168.1.1".parse::<IPv4Address>().unwrap().subnet.is_none());
        assert_eq!("192.168.1.1".parse::<IPv4Address>().unwrap().value, [192,168,1,1]);
        assert_eq!("192.168.2.1".parse::<IPv4Address>().unwrap().value, [192,168,2,1]);
        assert_eq!("192.16.8.2.1".parse::<IPv4Address>().unwrap_err(), AddressParsingError::IncorrectStringFormat);
        assert_eq!("a920::0001".parse::<IPv4Address>().unwrap_err(), AddressParsingError::IncorrectStringFormat);
        assert_eq!("371.168.0.1".parse::<IPv4Address>().unwrap_err(), AddressParsingError::AddressGroupOutOfBounds);
    }

    #[test]
    fn ipv4addresssubnet_parse_from_string() {
        assert_eq!("192.168.1.1/24".parse::<IPv4Address>().unwrap().subnet.unwrap().bits, 24);
        assert_eq!("192.168.2.1/12".parse::<IPv4Address>().unwrap().subnet.unwrap().bits, 12);
        assert_eq!("192.168.2.1/37".parse::<IPv4Address>().unwrap_err(), AddressParsingError::Subnet(SubnetParsingError::SubnetOutOfRange));
        assert_eq!("192.168.2.1/ac".parse::<IPv4Address>().unwrap_err(), AddressParsingError::IncorrectStringFormat);
    }

}