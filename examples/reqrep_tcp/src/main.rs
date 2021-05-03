//!# RustyMQ Request-Reply TCP example
//!This is an example package that is an introduction on how to use RustyMQ.
//!The executable is able to act as a server (Reply socket) that can receive operation requests
//!for the addition or multiplication of two natural numbers then execute them and then reply with the result.
//!
//!It is also able to run as a client that will generate random addition and multiplication tasks of natural numbers,
//!and will also calulate the round trip time of the request. 
//!
//!## Usage
//!### Run as server
//!`<executable> --mode server --address <binding address>`
//!
//!### Run as client
//!`<executable> --mode client --address <server bound address>`

extern crate clap;
extern crate rustymq;
extern crate rand;
extern crate lazy_static;

use rustymq::base::Socket;

mod time;
mod data;
mod server;
mod client;


fn main() {
    use rustymq::base::{TransportMethod, NetworkAddress, TransportConfiguration,
                        MessageQueueOverflowHandling, MessageQueueingPolicy};
    use rustymq::model::{ReplySocket, RequestSocket};
    use rustymq::transport::network::tcp::{InitiatorTransport, AcceptorTransport, StreamConnectionBuilder, StreamListenerBuilder};

    // Create an argument parser for out application
    use clap::{Arg, App};
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
    // Create a TransportMethhod from the incoming argument parameter
    let transport_method: TransportMethod = NetworkAddress::from_dns(arguments.value_of("address").unwrap().to_string()).unwrap().into();
    
    // Start application in server mode
    if arguments.value_of("mode").unwrap() == "server".to_string() {
        // Creating a reply socket that will be used by `server::OperationServer` to receive messages on
        let socket = ReplySocket::new(
            // Create an `AcceptorTransport` to be used by the socket as the underlieing communication method.
            // Also create it with a custom configuration
            AcceptorTransport::with_configuration(
                // Create a new stream connection builder. It handles the configuration of the socket when connecting with another peer
                StreamConnectionBuilder::new(),
                // Create a new stream listener builder. Responsible for the configuration of the listener socket when listening for new peers
                StreamListenerBuilder::new(),
                         // Provide a transport configuration as well to finetune the operation
                         TransportConfiguration::new().with_queue_policy(
                                    // Set up internal message queueing policy so when internal queue has reached depth of 100 then throttle even NoWait messages
                                    MessageQueueingPolicy::default().with_overflow(Some((MessageQueueOverflowHandling::Throttle, 100)))))).unwrap();
        // Bind the socket to a given transport method in this case a network address and port
        socket.bind(transport_method).unwrap();
        // Create the operation server
        let server = server::OperationServer::new(socket);
        // Run the server with 8 threads
        server.execute_threads(8);
    // Start application in client mode
    } else if arguments.value_of("mode").unwrap() == "client".to_string() {
        // Creating a request socket that will be used by `clieng::OperationClient` to send messages on
        let socket = RequestSocket::new(
            // Create an `InitiatorTransport` to be used by the socket as the underlieing communication method.
            // Also create it with a custom configuration
            InitiatorTransport::with_configuration(
                // Create a new stream connection builder. It handles the configuration of the socket when connecting with another peer
                StreamConnectionBuilder::new(),
                 // Set up internal message queueing policy so when internal queue has reached depth of 25 then throttle even NoWait messages
                 TransportConfiguration::new().with_queue_policy(MessageQueueingPolicy::default().with_overflow(Some((MessageQueueOverflowHandling::Throttle, 25)))))).unwrap();
        
        // Connect socket to the server
        socket.connect(transport_method).unwrap();
        // Create the client with the socket
        let client = client::OperationClient::new(socket);
        // Start executing client
        client.execute_client();
    } else {
        eprintln!("Provided mode unkown: {}", arguments.value_of("mode").unwrap())
    }
}
