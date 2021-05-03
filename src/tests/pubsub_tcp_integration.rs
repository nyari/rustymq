use super::common::*;

use crate::base::{
    InwardSocket, Message, NetworkAddress, OpFlag, OutwardSocket, Socket, TransportMethod,
    TypedMessage,
};
use crate::model;
use crate::transport;

#[test]
fn simple_pub_sub_tcp_test() {
    let subscriber1 =
        model::pubsub::SubscriberSocket::new(transport::network::tcp::InitiatorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
        ));
    let subscriber2 =
        model::pubsub::SubscriberSocket::new(transport::network::tcp::InitiatorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
        ));
    let publisher1 =
        model::pubsub::PublisherSocket::new(transport::network::tcp::AcceptorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
            transport::network::tcp::StreamListenerBuilder::new(),
        ));
    let publisher2 =
        model::pubsub::PublisherSocket::new(transport::network::tcp::AcceptorTransport::new(
            transport::network::tcp::StreamConnectionBuilder::new(),
            transport::network::tcp::StreamListenerBuilder::new(),
        ));

    publisher1
        .bind(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46000".to_string()).unwrap(),
        ))
        .unwrap();
    publisher2
        .bind(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46001".to_string()).unwrap(),
        ))
        .unwrap();
    subscriber1
        .connect(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46000".to_string()).unwrap(),
        ))
        .unwrap();
    subscriber2
        .connect(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46000".to_string()).unwrap(),
        ))
        .unwrap();
    subscriber1
        .connect(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46001".to_string()).unwrap(),
        ))
        .unwrap();
    subscriber2
        .connect(TransportMethod::Network(
            NetworkAddress::from_dns("localhost:46001".to_string()).unwrap(),
        ))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));

    let base = TestingStruct { a: 5, b: 5 };
    let message = TypedMessage::new(base);

    publisher1
        .send_typed(message.clone(), OpFlag::Wait)
        .unwrap();
    publisher2
        .send_typed(message.clone(), OpFlag::Wait)
        .unwrap();

    assert_eq!(
        base,
        subscriber1
            .receive_typed::<TestingStruct>(OpFlag::Wait)
            .unwrap()
            .into_payload()
    );
    assert_eq!(
        base,
        subscriber2
            .receive_typed::<TestingStruct>(OpFlag::Wait)
            .unwrap()
            .into_payload()
    );
    assert_eq!(
        base,
        subscriber1
            .receive_typed::<TestingStruct>(OpFlag::Wait)
            .unwrap()
            .into_payload()
    );
    assert_eq!(
        base,
        subscriber2
            .receive_typed::<TestingStruct>(OpFlag::Wait)
            .unwrap()
            .into_payload()
    );
}
