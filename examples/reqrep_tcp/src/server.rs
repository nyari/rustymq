//! This module contains the functionality for executing a server that handled operations for a client
//! capable of handling requests on multiple threads

/// Importing to handle TypedMessages from RustyMQ
use rustymq::core::{Message, TypedMessage};
use rustymq::core::socket::{BidirectionalSocket, OpFlag, ArcSocket, SocketError, QueryTypedError, ReceiveTypedError};
use std::thread;

use super::data;

/// Implementation of a server that accepts messages from a Socket and handles [`data::OperationTask`]s.
pub struct OperationServer<Socket>
    where Socket: BidirectionalSocket + 'static {
    socket: ArcSocket<Socket>
}

impl<Socket> OperationServer<Socket>
    where Socket: BidirectionalSocket + 'static {
    /// Create a new OperationServer that listents for tasks on the `socket`
    pub fn new(socket: Socket) -> Self {
        Self {
            socket: ArcSocket::new(socket)
        }
    }

    /// Start executing the server on `threadcount` number of threads
    pub fn execute_threads(self, threadcount: u8) {
        // Fill a vector with thread handles
        let threads_handles = {
            let mut result: Vec<thread::JoinHandle<()>> = Vec::new();

            for _count in 0..threadcount {
                let arc_socket = self.socket.clone();
                // Create threads that handle new messages
                result.push(thread::spawn(move || {
                    loop {
                        arc_socket.respond_typed(OpFlag::Wait, OpFlag::NoWait, |message: TypedMessage<data::TimedOperation<data::OperationTask>> | {
                            let (metadata, input) = message.into_parts();
                            let (peer_id, conversation_id) = (metadata.peer_id().unwrap().clone(), metadata.conversation_id().clone());

                            println!("Received: Peer: {}\tConversation: {}", peer_id.get(), conversation_id.get());

                            let response = match input.0 {
                                data::OperationTask::Addition(v1, v2) => data::OperationResult::Success(v1 + v2),
                                data::OperationTask::Multiplication(v1, v2) => data::OperationResult::Success(v1 * v2)
                            };

                            println!("Responding: Peer: {}\tConversation: {}", peer_id.get(), conversation_id.get());

                            TypedMessage::new(data::TimedOperation(response, input.1)).continue_exchange_metadata(metadata)
                        }).or_else(|err| { 
                                if let QueryTypedError::Receive(ReceiveTypedError::Socket((_, SocketError::Timeout))) = err {
                                    std::thread::sleep(std::time::Duration::from_millis(1)); Ok(())
                                } else {
                                    Err(err)
                                }
                            })
                        .unwrap();
                    }
                }))
            }

            result
        };

        // Wait for all the threads to finish (this will not happen with the current code)
        threads_handles.into_iter().for_each(|handle| {handle.join().unwrap()});
    }
}
