use rustymq::core::Socket;

extern crate clap;
extern crate rustymq;
extern crate rand;

/// # Data module
/// This module is used to define the datastructure between the client and server and implements serialization
mod data {
    use std::convert::TryFrom;

    /// For serialization in this example the internal serializer of RusyMQ is used. Although it is possible to use
    /// any serializaton method or library.
    use rustymq::core::serializer::{Serializable, Serializer, Deserializer, FlatSerializer, FlatDeserializer, Error, Buffer};

    #[derive(Debug)]
    /// # Operation Task
    /// Is the communication structure used to comminucate an operation task to the server
    pub enum OperationTask {
        /// Add two 32-bit integers
        Addition(i32, i32),
        /// Multiply two 32-bit integers
        Multiplication(i32, i32)
    }

    #[derive(Debug)]
    /// # Operation result
    /// Is the communication structure used to communicate the result of an [`OperationTask`] to the client
    pub enum OperationResult {
        Success(i32),
        Error
    }

    /// Implement serialization for [`OperationTask`]
    impl Serializable for OperationTask {
        fn serialize<T:Serializer>(&self, serializer: &mut T) {
            match self {
                OperationTask::Addition(v1,v2 ) => {
                    serializer.serialize_raw(&0u8);
                    serializer.serialize_raw(v1);
                    serializer.serialize_raw(v2);
                },
                OperationTask::Multiplication(v1, v2) => {
                    serializer.serialize_raw(&1u8);
                    serializer.serialize_raw(v1);
                    serializer.serialize_raw(v2);

                }
            }
        }
        fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
            match deserializer.deserialize_raw::<u8>()? {
                0u8 => {
                    Ok(Self::Addition(deserializer.deserialize_raw::<i32>()?, deserializer.deserialize_raw::<i32>()?))
                },
                1u8 => {
                    Ok(Self::Multiplication(deserializer.deserialize_raw::<i32>()?, deserializer.deserialize_raw::<i32>()?))
                },
                _ => Err(Error::DemarshallingFailed)
            }
        }
    }

    /// Implement serialization for [`OperationTask`]
    impl Serializable for OperationResult {
        fn serialize<T:Serializer>(&self, serializer: &mut T) {
            match self {
                OperationResult::Success(v1 ) => {
                    serializer.serialize_raw(&0u8);
                    serializer.serialize_raw(v1);
                },
                OperationResult::Error => {
                    serializer.serialize_raw(&1u8);
                }
            }
        }
        fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
            match deserializer.deserialize_raw::<u8>()? {
                0u8 => {
                    Ok(Self::Success(deserializer.deserialize_raw::<i32>()?))
                },
                1u8 => {
                    Ok(Self::Error)
                },
                _ => Err(Error::DemarshallingFailed)
            }
        }
    }

    /// Implement the TryFrom<Buffer> for OperationTask to be able to use it in [`TypedMessage`]s. 
    impl TryFrom<Buffer> for OperationTask {
        type Error = Error;
        fn try_from(value: Buffer) -> Result<Self, Self::Error> {
            let mut deserializer = FlatDeserializer::new(value.as_slice())?;
            deserializer.deserialize::<Self>()
        }
    }

    /// Implement the TryFrom<Buffer> for OperationTask to be able to use it in [`TypedMessage`]s. 
    impl TryFrom<OperationTask> for Buffer {
        type Error = Error;
        fn try_from(value: OperationTask) -> Result<Self, Self::Error> {
            let mut serializer = FlatSerializer::new();
            serializer.serialize_pass(value);
            Ok(serializer.finalize())
        }
    }

    /// Implement the TryFrom<Buffer> for OperationResult to be able to use it in [`TypedMessage`]s. 
    impl TryFrom<Buffer> for OperationResult {
        type Error = Error;
        fn try_from(value: Buffer) -> Result<Self, Error> {
            let mut deserializer = FlatDeserializer::new(value.as_slice())?;
            deserializer.deserialize::<Self>()
        }
    }

    /// Implement the TryFrom<Buffer> for OperationResult to be able to use it in [`TypedMessage`]s. 
    impl TryFrom<OperationResult> for Buffer {
        type Error = Error;
        fn try_from(value: OperationResult) -> Result<Self, Error> {
            let mut serializer = FlatSerializer::new();
            serializer.serialize_pass(value);
            Ok(serializer.finalize())
        }
    }
}


/// This module contains the functionality for executing a server that handled operations for a client
/// capable of handling requests on multiple threads
mod server {
    /// Importing to handle TypedMessages from RustyMQ
    use rustymq::core::{Message, TypedMessage};
    use rustymq::core::socket::{BidirectionalSocket, OpFlag, SocketError, ReceiveTypedError};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use super::data;

    /// Implementation of a server that accepts messages from a Socket and handles [`data::OperationTask`]s.
    pub struct OperationServer<Socket>
        where Socket: BidirectionalSocket + 'static {
        socket: Arc<Mutex<Socket>>
    }

    impl<Socket> OperationServer<Socket>
        where Socket: BidirectionalSocket + 'static {
        /// Create a new OperationServer that listents for tasks on the `socket`
        pub fn new(socket: Socket) -> Self {
            Self {
                socket: Arc::new(Mutex::new(socket))
            }
        }

        /// Start executing the server on `threadcount` number of threads
        pub fn execute_threads(self, threadcount: u8) {
            // Fill a vector with thread handles
            let threads_handles = {
                let mut result: Vec<thread::JoinHandle<()>> = Vec::new();

                for _count in 0..threadcount {
                    let socket_locked= self.socket.clone();
                    // Create threads that handle new messages
                    result.push(thread::spawn(move || {
                        loop {
                            // Receive message from the `socket` and break it into metadata and the input 
                            let (metadata, input) = rustymq::core::util::thread::wait_for(std::time::Duration::from_millis(1), || {
                                let mut socket = socket_locked.lock().unwrap();
                                // Receive the actual message. Though the error handling is not implemented here. Any error (i. e. client disconnect)
                                // will cause a panic because of the unwrap.
                                match socket.receive_typed::<data::OperationTask>(OpFlag::NoWait) {
                                    Ok(message) => Some(message),
                                    Err(ReceiveTypedError::Socket((_, SocketError::Timeout))) => None,
                                    _ => panic!("Unhandled error case")
                                }
                            }).into_parts();

                            println!("Received: Peer: {}\tConversation: {}", metadata.peer_id().unwrap().get(), metadata.conversation_id().get());

                            // Generate the result of a [`data::OperationTask`] into a [`data::OperationResult`]
                            let response = match input {
                                data::OperationTask::Addition(v1, v2) => data::OperationResult::Success(v1 + v2),
                                data::OperationTask::Multiplication(v1, v2) => data::OperationResult::Success(v1 * v2)
                            };

                            // Send response through a socket
                            {
                                let mut socket = socket_locked.lock().unwrap();
                                // Send the actual message. Though the error handling is not implemented here. Any error (i. e. client disconnect)
                                // will cause a panic because of the unwrap.
                                let response_metadata = socket.send_typed::<data::OperationResult>(
                                    // Create a typed message with the modified metadata
                                    TypedMessage::with_metadata(
                                        // Continue exchange on metadata (will keep conversation id but generate a new message id)
                                        metadata.continue_exchange(),
                                        // Set the response in the message as well
                                        response),
                                    // Set OpFlag to NoWait because we do not need to wait for the message to be entirely sent
                                    OpFlag::NoWait).unwrap();
                                println!("Responded: Peer: {}\tConversation: {}", response_metadata.peer_id().unwrap().get(), response_metadata.conversation_id().get());
                            }
                        }
                    }))
                }

                result
            };

            // Wait for all the threads to finish (this will not happen with the current code)
            threads_handles.into_iter().for_each(|handle| {handle.join().unwrap()});
        }
    }
}

/// This module contains the functionality for executing a client that sends operation to the server
/// capable of handling requests on multiple threads
mod client {
    use rand::Rng;
    /// Importing to handle TypedMessages from RustyMQ
    use rustymq::core::message::{TypedMessage, Message};
    use rustymq::core::socket::{BidirectionalSocket, OpFlag};

    use super::data;

    /// Implementation of a server that accepts messages from a Socket and handles [`data::OperationTask`]s.
    pub struct OperationClient<Socket>
        where Socket: BidirectionalSocket + 'static {
        socket: Socket
    }

    impl<Socket> OperationClient<Socket>
        where Socket: BidirectionalSocket + 'static {
        /// Create a new OperationServer that listents for tasks on the `socket`
        pub fn new(socket: Socket) -> Self {
            Self {
                socket: socket
            }
        }

        /// Start executing the server on `threadcount` number of threads
        pub fn execute_client(mut self) {
            let mut rnd = rand::thread_rng();
            loop {
                let operation = match rnd.gen_range(0, 1) {
                    0 => {
                        data::OperationTask::Addition(rnd.gen_range(-1000, 1000), rnd.gen_range(-1000, 1000))
                    },
                    1 => {
                        data::OperationTask::Multiplication(rnd.gen_range(-1000, 1000), rnd.gen_range(-1000, 1000))
                    },
                    _ => panic!("Impossible random")
                };
                let message = TypedMessage::new(operation);
                println!("Request: Conversation: {}", message.conversation_id().get());
                self.socket.send_typed(message, OpFlag::Default).unwrap();
                let result = self.socket.receive_typed::<data::OperationResult>(OpFlag::Default).unwrap();
                println!("Response: Conversation: {}", result.conversation_id().get());
            }
        }
    }
}

fn main() {
    use clap::{Arg, App};
    use rustymq::core::transport::{TransportMethod, NetworkAddress};
    use rustymq::model::reqrep::{ReplySocket, RequestSocket};
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
        let mut socket = ReplySocket::new(AcceptorTransport::new(StreamConnectionBuilder::new(), StreamListenerBuilder::new()));
        socket.bind(transport_method).unwrap();
        let server = server::OperationServer::new(socket);
        server.execute_threads(1);
    } else if arguments.value_of("mode").unwrap() == "client".to_string() {
        let mut socket = RequestSocket::new(InitiatorTransport::new(StreamConnectionBuilder::new()));
        socket.connect(transport_method).unwrap();
        let client = client::OperationClient::new(socket);
        client.execute_client();
    } else {
        eprintln!("Provided mode unkown: {}", arguments.value_of("mode").unwrap())
    }
}