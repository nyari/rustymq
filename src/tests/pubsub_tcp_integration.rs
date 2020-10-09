pub use super::super::*;

use core::socket::{Socket, OpFlag, OutwardSocket, InwardSocket};
use core::serializer::{FlatDeserializer, FlatSerializer, Serializer, Deserializer};
use core::serializer;
use core::message::{TypedMessage, Buffer, Message};

use std::convert::{TryFrom};

#[derive(Debug)]
#[derive(PartialEq)]
#[derive(Clone)]
#[derive(Copy)]
struct TestingStruct {
    pub a: u64,
    pub b: u64
}

impl TryFrom<TestingStruct> for Buffer {
    type Error = ();
    fn try_from(value: TestingStruct) -> Result<Self, Self::Error> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize_raw(&value.a);
        serializer.serialize_raw(&value.b);
        Ok(serializer.finalize())
    }
}

impl TryFrom<Buffer> for TestingStruct {
    type Error = serializer::Error;
    fn try_from(value: Buffer) -> Result<Self, Self::Error> {
        let mut deserializer = FlatDeserializer::new(value.as_slice())?;

        Ok(Self {
            a: deserializer.deserialize_raw::<u64>()?,
            b: deserializer.deserialize_raw::<u64>()?
        })
    }
}


#[test]
fn simple_pub_sub_tcp_test() {
    let mut subscriber1 = model::pubsub::SubscriberSocket::new(transport::network::TCPInitiatorTransport::new());
    let mut subscriber2 = model::pubsub::SubscriberSocket::new(transport::network::TCPInitiatorTransport::new());
    let mut publisher = model::pubsub::PublisherSocket::new(transport::network::TCPAcceptorTransport::new());

    publisher.bind(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 46000))).unwrap();
    subscriber1.connect(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 46000))).unwrap();
    subscriber2.connect(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 46000))).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));

    let base = TestingStruct{a: 5, b: 5};
    let message = TypedMessage::new(base);

    publisher.send_typed(message, OpFlag::NoWait).unwrap();

    assert_eq!(base, subscriber1.receive_typed::<TestingStruct>(OpFlag::Default).unwrap().into_payload());
    assert_eq!(base, subscriber2.receive_typed::<TestingStruct>(OpFlag::Default).unwrap().into_payload());
}