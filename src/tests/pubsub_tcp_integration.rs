pub use super::super::*;

use super::common::*;
use core::socket::{Socket, OpFlag, OutwardSocket, InwardSocket};
use core::transport::{NetworkAddress};
use core::message::{TypedMessage, Message};

#[test]
fn simple_pub_sub_tcp_test() {
    let mut subscriber1 = model::pubsub::SubscriberSocket::new(transport::network::tcp::InitiatorTransport::new(transport::network::tcp::StreamConnectionBuilder::new()));
    let mut subscriber2 = model::pubsub::SubscriberSocket::new(transport::network::tcp::InitiatorTransport::new(transport::network::tcp::StreamConnectionBuilder::new()));
    let mut publisher1 = model::pubsub::PublisherSocket::new(transport::network::tcp::AcceptorTransport::new(transport::network::tcp::StreamConnectionBuilder::new(), transport::network::tcp::StreamListenerBuilder::new()));
    let mut publisher2 = model::pubsub::PublisherSocket::new(transport::network::tcp::AcceptorTransport::new(transport::network::tcp::StreamConnectionBuilder::new(), transport::network::tcp::StreamListenerBuilder::new()));

    publisher1.bind(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46000".to_string()).unwrap())).unwrap();
    publisher2.bind(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46001".to_string()).unwrap())).unwrap();
    subscriber1.connect(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46000".to_string()).unwrap())).unwrap();
    subscriber2.connect(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46000".to_string()).unwrap())).unwrap();
    subscriber1.connect(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46001".to_string()).unwrap())).unwrap();
    subscriber2.connect(core::TransportMethod::Network(NetworkAddress::from_dns("localhost:46001".to_string()).unwrap())).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));

    let base = TestingStruct{a: 5, b: 5};
    let message = TypedMessage::new(base);

    publisher1.send_typed(message.clone(), OpFlag::Wait).unwrap();
    publisher2.send_typed(message.clone(), OpFlag::Wait).unwrap();

    assert_eq!(base, subscriber1.receive_typed::<TestingStruct>(OpFlag::Wait).unwrap().into_payload());
    assert_eq!(base, subscriber2.receive_typed::<TestingStruct>(OpFlag::Wait).unwrap().into_payload());
    assert_eq!(base, subscriber1.receive_typed::<TestingStruct>(OpFlag::Wait).unwrap().into_payload());
    assert_eq!(base, subscriber2.receive_typed::<TestingStruct>(OpFlag::Wait).unwrap().into_payload());
}