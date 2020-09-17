pub use super::super::*;

use core::socket::{Socket, SocketError, OpFlag, OutwardSocket, InwardSocket, BidirectionalSocket,
                   QueryTypedError, ReceiveTypedError};
use core::serializer::{FlatDeserializer, FlatSerializer, Serializer, Deserializer};
use core::serializer;
use core::message::{TypedMessage, Buffer, Message};
use std::collections::{HashMap};
use std::sync::{Arc, Mutex};

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
fn simple_req_rep_tcp_test() {
    let mut requestor = model::reqrep::RequestSocket::new(transport::network::TCPInitiatorTransport::new());
    let mut replier = model::reqrep::ReplySocket::new(transport::network::TCPAcceptorTransport::new());

    replier.bind(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 45321))).unwrap();
    requestor.connect(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 45321))).unwrap();

    let base = TestingStruct{a: 5, b: 5};
    let message = TypedMessage::new(base);

    requestor.send_typed(message, OpFlag::NoWait).unwrap();
    replier.respond_typed(OpFlag::Default, |rmessage:TypedMessage<TestingStruct>| {
        TypedMessage::new(rmessage.payload().clone()).continue_exchange_metadata(rmessage.into_metadata())
    }).unwrap();
    let final_message = requestor.receive_typed::<TestingStruct>(OpFlag::Default).expect("Hello");

    assert_eq!(base, final_message.into_payload());
}

#[test]
fn stress_simple_req_rep_tcp_test() {
    let mut requestor = model::reqrep::RequestSocket::new(transport::network::TCPInitiatorTransport::new());
    let mut replier = model::reqrep::ReplySocket::new(transport::network::TCPAcceptorTransport::new());

    replier.bind(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 48000))).unwrap();
    requestor.connect(core::TransportMethod::Network(std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)), 48000))).unwrap();

    let stop_semaphore = Arc::new(Mutex::new(false));
    let stop_semaphore_clone = stop_semaphore.clone();
    let _replier_handle = std::thread::spawn(move || { 
        loop {
            match replier.respond_typed(OpFlag::NoWait, |rmessage:TypedMessage<TestingStruct>| {
                let (metadata, mut payload) = rmessage.into_parts();
                payload.a *= 2;
                payload.b *= 7;
                TypedMessage::new(payload).continue_exchange_metadata(metadata)
            }) {
                Ok(()) | Err(QueryTypedError::Receive(ReceiveTypedError::Socket(SocketError::Timeout))) => {
                    if *stop_semaphore.lock().unwrap() {
                        break;
                    }
                },
                _ => panic!("")
            }
        }
    });

    let mut messages = HashMap::new();

    for index in 0..1000 {
        let base = TestingStruct{a: index, b: index};
        let message = TypedMessage::new(base);
        messages.insert(message.conversation_id().clone(), base);
        requestor.send_typed(message, OpFlag::NoWait).unwrap();
    }

    for _index in 0..1000 {
        let final_message = requestor.receive_typed::<TestingStruct>(OpFlag::Default).expect("Hello");
        let (metadata, payload) = final_message.into_parts();
        let original_payload = messages.get(metadata.conversation_id()).unwrap();
        assert_eq!(original_payload.a * 2, payload.a);
        assert_eq!(original_payload.b * 7, payload.b);
    }

    *stop_semaphore_clone.lock().unwrap() = true;
    _replier_handle.join().unwrap();
}