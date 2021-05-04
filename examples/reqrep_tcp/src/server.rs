//! This module contains the functionality for executing a server that handled operations for a client
//! capable of handling requests on multiple threads

/// Importing to handle TypedMessages from RustyMQ
use rustymq::base::{Message, TypedMessage, BidirectionalSocket, OpFlag, ArcSocket};
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

            // Start `threadcount` number of threads
            for _count in 0..threadcount {
                let arc_socket = self.socket.clone();
                // Create threads that handle new messages
                result.push(thread::spawn(move || {
                    // Infinite loop to handle messages
                    loop {
                        // Use the `respond_typed` method to respond to messages
                        // We are using OpFlag::Wait for incoming messages. This means that the thread is blocked until a message is received
                        // Bet we use OpFlag::NoWait for sending since we do not need to wait until the message is written to the socket
                        arc_socket.respond_typed(OpFlag::Wait, OpFlag::NoWait, |message: TypedMessage<data::TimedOperation<data::OperationTask>> | {
                            // Split message into metadata and payload
                            let (metadata, input) = message.into_parts();
                            // Copy the `peer_id` and `conversation_id` from the metadata
                            let (peer_id, conversation_id) = (metadata.peer_id().unwrap().clone(), metadata.conversation_id().clone());

                            // Print out peer and conversation id
                            println!("Received:   Peer: {}\tConversation: {}", peer_id.get(), conversation_id.get());

                            // Calculate the response to the message
                            let response = match input.0 {
                                data::OperationTask::Addition(v1, v2) => data::OperationResult::Success(v1 + v2),
                                data::OperationTask::Multiplication(v1, v2) => data::OperationResult::Success(v1 * v2)
                            };

                            // Print out peer and conversation id
                            println!("Responding: Peer: {}\tConversation: {}", peer_id.get(), conversation_id.get());

                            // Create the response for the message
                            TypedMessage::new(data::TimedOperation(response, input.1))
                                // We have to explicitly continue the conversion with the metadata
                                // this is needed because the internal message tracking of the `ReplySocket` will result in an error
                                // if the `conversation_id` from the original metadata is not carried forward
                                .continue_exchange_metadata(metadata)
                        }).unwrap();
                    }
                }))
            }

            result
        };

        // Wait for all the threads to finish (this will not happen with the current code)
        threads_handles.into_iter().for_each(|handle| {handle.join().unwrap()});
    }
}
