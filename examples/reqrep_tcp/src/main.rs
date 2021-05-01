extern crate clap;
extern crate rustymq;
extern crate rand;
extern crate lazy_static;

use rustymq::core::Socket;

mod time;
mod data;
mod server;
mod client;


fn main() {
    use clap::{Arg, App};
    use rustymq::core::transport::{TransportMethod, NetworkAddress};
    use rustymq::model::reqrep::{ReplySocket, RequestSocket};
    use rustymq::core::config::{TransportConfiguration};
    use rustymq::core::queue::{MessageQueueOverflowHandling, MessageQueueingPolicy};
    use rustymq::transport::network::tcp::{InitiatorTransport, AcceptorTransport, StreamConnectionBuilder, StreamListenerBuilder};

    let arguments = App::new("RustyMQ Request-Reply TCP Example")
                        .author("David Tamas Nyari <dev@dnyari.info>")
                        .about("Simple client-sercer example to learn of RustyMQ")
                        .arg(Arg::with_name("mode")
                                .short("m")
                                .long("mode")
                                .value_name("MODE")
                                .required(true)
                                .help("Either server or client"))
                        .arg(Arg::with_name("address")
                                .short("a")
                                .long("address")
                                .value_name("ADDRESS")
                                .required(true)
                                .help("Set the address to connect to or listen on"))
                        .get_matches();
    let transport_method = TransportMethod::Network(NetworkAddress::from_dns(arguments.value_of("address").unwrap().to_string()).unwrap());
    
    if arguments.value_of("mode").unwrap() == "server".to_string() {
        let socket = ReplySocket::new(AcceptorTransport::with_configuration(StreamConnectionBuilder::new(),
                                                                                StreamListenerBuilder::new(),
                                                                                TransportConfiguration::new().with_queue_policy(MessageQueueingPolicy::default().with_overflow(Some((MessageQueueOverflowHandling::Throttle, 100)))))).unwrap();
        socket.bind(transport_method).unwrap();
        let server = server::OperationServer::new(socket);
        server.execute_threads(8);
    } else if arguments.value_of("mode").unwrap() == "client".to_string() {
        let socket = RequestSocket::new(InitiatorTransport::with_configuration(StreamConnectionBuilder::new(),
                                                                                   TransportConfiguration::new().with_queue_policy(MessageQueueingPolicy::default().with_overflow(Some((MessageQueueOverflowHandling::Throttle, 25)))))).unwrap();
        socket.connect(transport_method).unwrap();
        let client = client::OperationClient::new(socket);
        client.execute_client();
    } else {
        eprintln!("Provided mode unkown: {}", arguments.value_of("mode").unwrap())
    }
}
