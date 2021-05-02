use core::message::RawMessage;
use core::stream::header::{HeadedMessage, Header, HeaderOperation};

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rand;

struct Tracked(u32, Duration);

#[derive(Debug, Clone, Copy)]
pub enum TrackingError {
    ReceiptError,
}

pub struct Tracker {
    sequence: u32,
    begin_instant: Instant,
    outward_track: VecDeque<Tracked>,
}

impl Tracker {
    pub fn new() -> Self {
        Self {
            sequence: rand::random(),
            begin_instant: Instant::now(),
            outward_track: VecDeque::new(),
        }
    }

    pub fn head_message(&mut self, message: RawMessage) -> HeadedMessage {
        let sequence = self.next_sequence();
        self.outward_track
            .push_back(Tracked(sequence, self.begin_instant.elapsed()));
        HeadedMessage::new(Header::new(sequence), HeaderOperation::Payload(message))
    }

    pub fn head_message_and_receipt(
        &mut self,
        message: RawMessage,
        header: Header,
    ) -> HeadedMessage {
        let sequence = self.next_sequence();
        self.outward_track
            .push_back(Tracked(sequence, self.begin_instant.elapsed()));
        HeadedMessage::new(
            Header::new(sequence),
            HeaderOperation::ReceiptWithNextPayload(header, message),
        )
    }

    pub fn unwrap_incoming_message(
        &mut self,
        headed_message: HeadedMessage,
    ) -> Result<(Option<RawMessage>, Option<HeadedMessage>), TrackingError> {
        let (header, op) = headed_message.into_parts();
        match op {
            HeaderOperation::Payload(message) => Ok((
                Some(message),
                Some(HeadedMessage::new(header, HeaderOperation::Receipt)),
            )),
            HeaderOperation::Heartbeat => Ok((
                None,
                Some(HeadedMessage::new(header, HeaderOperation::Receipt)),
            )),
            HeaderOperation::Receipt => {
                let Tracked(sequence, _dur) = self
                    .outward_track
                    .pop_front()
                    .ok_or(TrackingError::ReceiptError)?;
                if header.sequence() == sequence {
                    Ok((None, None))
                } else {
                    Err(TrackingError::ReceiptError)
                }
            }
            HeaderOperation::ReceiptWithNextPayload(receipt_header, message) => {
                let Tracked(sequence, _dur) = self
                    .outward_track
                    .pop_front()
                    .ok_or(TrackingError::ReceiptError)?;
                if receipt_header.sequence() == sequence {
                    Ok((
                        Some(message),
                        Some(HeadedMessage::new(header, HeaderOperation::Receipt)),
                    ))
                } else {
                    Err(TrackingError::ReceiptError)
                }
            }
        }
    }

    fn next_sequence(&mut self) -> u32 {
        let result = self.sequence;
        self.sequence += 1;
        result
    }
}
