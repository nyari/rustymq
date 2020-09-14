pub use super::*;

use core::socket::{Socket, OpFlag, OutwardSocket, InwardSocket, BidirectionalSocket};
use core::serializer::{FlatDeserializer, FlatSerializer, Serializer, Deserializer};
use core::serializer;
use core::message::{TryIntoFromBuffer, TypedMessage, Buffer, Message};
use std::ops::{Deref, DerefMut};

use std::convert::{TryInto, TryFrom};

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

impl TryIntoFromBuffer for TestingStruct {

}


#[test]
fn simple_req_rep_tcp_test() {
    let mut requestor = model::reqrep::RequestSocket::new(transport::network::TCPInitiatorTransport::new());
    let mut replier = model::reqrep::ReplySocket::new(transport::network::TCPAcceptorTransport::new());

    replier.bind(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 45321))).unwrap();
    requestor.connect(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 45321))).unwrap();

    let base = TestingStruct{a: 5, b: 5};
    let message = TypedMessage::new(base);

    requestor.send_typed(message, OpFlag::NoWait).unwrap();
    let received_message = replier.receive_typed::<TestingStruct>(OpFlag::Default).expect("Hello");
    replier.send_typed(TypedMessage::new(received_message.payload().clone()).continue_exchange_metadata(received_message.metadata().clone()), OpFlag::NoWait).expect("Hello");
    let final_message = requestor.receive_typed::<TestingStruct>(OpFlag::Default).expect("Hello");

    assert_eq!(base, final_message.into_payload());
}