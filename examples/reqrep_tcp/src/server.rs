//! This module contains the functionality for executing a server that handled operations for a client
//! capable of handling requests on multiple threads

/// Importing to handle TypedMessages from RustyMQ
use rustymq::core::{Message, TypedMessage};
use rustymq::core::socket::{BidirectionalSocket, InwardSocket, OutwardSocket, OpFlag, ArcSocket};
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
                let mut arc_socket = self.socket.clone();
                // Create threads that handle new messages
                result.push(thread::spawn(move || {
                    loop {
                        // Receive message from the `socket` and break it into metadata and the input. Implement it as a no waiting operation
                        let (metadata, input) = arc_socket.receive_typed::<data::OperationTask>(OpFlag::Wait).unwrap().into_parts();

                        println!("Received: Peer: {}\tConversation: {}", metadata.peer_id().unwrap().get(), metadata.conversation_id().get());

                        // Generate the result of a [`data::OperationTask`] into a [`data::OperationResult`]
                        let response = match input {
                            data::OperationTask::Addition(v1, v2) => data::OperationResult::Success(v1 + v2),
                            data::OperationTask::Multiplication(v1, v2) => data::OperationResult::Success(v1 * v2)
                        };

                        // Send the actual message. Though the error handling is not implemented here. Any error (i. e. client disconnect)
                        // will cause a panic because of the unwrap.
                        let response_metadata = arc_socket.send_typed::<data::OperationResult>(
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
                }))
            }

            result
        };

        // Wait for all the threads to finish (this will not happen with the current code)
        threads_handles.into_iter().for_each(|handle| {handle.join().unwrap()});
    }
}