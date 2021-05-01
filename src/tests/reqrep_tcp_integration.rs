pub use super::super::*;

use super::common::*;
use core::message::{Message, MessageMetadata, TypedMessage};
use core::socket::{
    BidirectionalSocket, InwardSocket, OpFlag, OutwardSocket, QueryTypedError, ReceiveTypedError,
    SendTypedError, Socket, SocketError,
};
use core::transport::NetworkAddress;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[test]
fn simple_req_rep_tcp_test() {
    let mut requestor =
        model::reqrep::RequestSocket::new(transport::network::tcp::InitiatorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
        ))
        .unwrap();
    let mut replier =
        model::reqrep::ReplySocket::new(transport::network::tcp::AcceptorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
            transport::network::tcp::StreamListenerBuilder::new(),
        ))
        .unwrap();

    replier
        .bind(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:45321".to_string()).unwrap(),
        ))
        .unwrap();
    requestor
        .connect(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:45321".to_string()).unwrap(),
        ))
        .unwrap();

    let base = TestingStruct { a: 5, b: 5 };
    let message = TypedMessage::new(base);

    requestor.send_typed(message, OpFlag::NoWait).unwrap();
    replier
        .respond_typed(
            OpFlag::Wait,
            OpFlag::Wait,
            |rmessage: TypedMessage<TestingStruct>| {
                TypedMessage::new(rmessage.payload().clone())
                    .continue_exchange_metadata(rmessage.into_metadata())
            },
        )
        .unwrap();
    let final_message = requestor
        .receive_typed::<TestingStruct>(OpFlag::Wait)
        .expect("Hello");

    assert_eq!(base, final_message.into_payload());
}

#[test]
fn stress_simple_req_rep_tcp_test() {
    let mut requestor =
        model::reqrep::RequestSocket::new(transport::network::tcp::InitiatorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
        ))
        .unwrap();
    let mut replier =
        model::reqrep::ReplySocket::new(transport::network::tcp::AcceptorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
            transport::network::tcp::StreamListenerBuilder::new(),
        ))
        .unwrap();

    replier
        .bind(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:48000".to_string()).unwrap(),
        ))
        .unwrap();
    requestor
        .connect(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:48000".to_string()).unwrap(),
        ))
        .unwrap();

    let stop_semaphore = Arc::new(Mutex::new(false));
    let stop_semaphore_clone = stop_semaphore.clone();
    let _replier_handle = std::thread::spawn(move || loop {
        match replier.respond_typed(
            OpFlag::NoWait,
            OpFlag::NoWait,
            |rmessage: TypedMessage<TestingStruct>| {
                let (metadata, mut payload) = rmessage.into_parts();
                payload.a *= 2;
                payload.b *= 7;
                TypedMessage::new(payload).continue_exchange_metadata(metadata)
            },
        ) {
            Ok(())
            | Err(QueryTypedError::Receive(ReceiveTypedError::Socket((
                None,
                SocketError::Timeout,
            )))) => {
                if *stop_semaphore.lock().unwrap() {
                    break;
                }
            }
            _ => panic!(""),
        }
    });

    let mut messages = HashMap::new();

    for index in 0..1000 {
        let base = TestingStruct { a: index, b: index };
        let message = TypedMessage::new(base);
        messages.insert(message.conversation_id().clone(), base);
        requestor.send_typed(message, OpFlag::NoWait).unwrap();
    }

    for _index in 0..1000 {
        let final_message = requestor
            .receive_typed::<TestingStruct>(OpFlag::Wait)
            .expect("Hello");
        let (metadata, payload) = final_message.into_parts();
        let original_payload = messages.get(metadata.conversation_id()).unwrap();
        assert_eq!(original_payload.a * 2, payload.a);
        assert_eq!(original_payload.b * 7, payload.b);
    }

    *stop_semaphore_clone.lock().unwrap() = true;
    _replier_handle.join().unwrap();
}

#[test]
fn simple_req_rep_tcp_test_disconnected_before_first_send() {
    let mut requestor =
        model::reqrep::RequestSocket::new(transport::network::tcp::InitiatorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
        ))
        .unwrap();
    let mut replier =
        model::reqrep::ReplySocket::new(transport::network::tcp::AcceptorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
            transport::network::tcp::StreamListenerBuilder::new(),
        ))
        .unwrap();

    replier
        .bind(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:45322".to_string()).unwrap(),
        ))
        .unwrap();
    requestor
        .connect(core::TransportMethod::Network(
            NetworkAddress::from_dns("localhost:45322".to_string()).unwrap(),
        ))
        .unwrap();

    let base = TestingStruct { a: 5, b: 5 };
    let message = TypedMessage::new(base);

    replier.close().unwrap();

    assert!(matches!(
        requestor.send_typed(message.clone(), OpFlag::Wait),
        Ok(_)
    ));
    assert!(matches!(
        requestor.send_typed(
            message
                .clone()
                .mutated_metadata(|_x| { MessageMetadata::new() }),
            OpFlag::Wait
        ),
        Err(SendTypedError::Socket(SocketError::Disconnected))
    ));
    assert!(matches!(
        requestor.send_typed(
            message
                .clone()
                .mutated_metadata(|_x| { MessageMetadata::new() }),
            OpFlag::Wait
        ),
        Err(SendTypedError::Socket(SocketError::Disconnected))
    ));
    //assert!(matches!(requestor.send_typed(message.clone(), OpFlag::Wait), Err(SendTypedError::Socket(SocketInternalError::Disconnected))));
}
