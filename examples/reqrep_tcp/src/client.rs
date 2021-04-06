//! This module contains the functionality for executing a client that sends operation to the server
//! capable of handling requests on multiple threads

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
            self.socket.send_typed(message, OpFlag::Wait).unwrap();
            while let Ok(result) = self.socket.receive_typed::<data::OperationResult>(OpFlag::NoWait) {
                println!("Response: Conversation: {}", result.conversation_id().get());
            } 
        }
    }
}