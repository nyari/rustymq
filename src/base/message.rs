//! # Message module
//! ## Summary
//! Datastructures, trait definitions for Message handling within RustyMQ
//! ## Details
//! This module containts the definition of Messages, which are a core concept of RustyMQ.
//! All the datastructures, traits and enums needed to compose messages for sending and then
//! receiving are defined here
use crate::base::info::Identifier;
use crate::internals::serializer;
use crate::internals::serializer::{
    Deserializer, FlatDeserializer, FlatSerializer, Serializable, Serializer,
};

use std;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};

const MESSAGE_METADATA_VERSION: u8 = 0u8;

/// General buffer type to be used in RusyMQ
pub type Buffer = Vec<u8>;
/// General buffer slice type to be used in RustyMQ
pub type BufferSlice<'a> = &'a [u8];
/// General buffer mutable slice type to be used in RustyMQ
pub type BufferMutSlice<'a> = &'a mut [u8];

/// # Multipart message part identifier
/// Enum for tracking multipart messages
/// ## Description
/// Enum to track multiplart message sequences more easily. Although the iteration of these should
/// be handled manually by the user
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Part {
    /// Signaling this is a single message
    Single,
    /// Signaling this is an intermediate message of a multipart message seqence and the index
    Intermediate(u32),
    /// Signaling this is a final message of a multipart message sequence and the index
    Final(u32),
}

impl Part {
    /// Generate new instance for a single message without multiple aprts
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let original = Part::single();
    /// assert!(std::matches!(original, Part::Single));
    /// # }
    /// ```
    pub fn single() -> Self {
        Part::Single
    }

    /// Generate a new instance for trackinga multipart message
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let original = Part::start_multipart();
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    /// # }
    /// ```
    pub fn start_multipart() -> Self {
        Part::Intermediate(0)
    }

    /// Query if part is multipart
    pub fn is_multipart(&self) -> bool {
        !std::matches!(self, Part::Single)
    }

    /// Query if continuable part
    pub fn is_continueable(&self) -> bool {
        match self {
            Part::Intermediate(_) => true,
            _ => false,
        }
    }

    /// Query if part can be used as a first message
    pub fn is_initial(&self) -> bool {
        match self {
            Part::Single | Part::Intermediate(0) => true,
            _ => false,
        }
    }

    /// Query if part can be used as the last message
    pub fn is_last(&self) -> bool {
        match self {
            Part::Single | Part::Final(_) => true,
            _ => false,
        }
    }

    /// Generate new instance for multipart message metadata with advancing the part id by one
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let original = Part::start_multipart();
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    ///
    /// let result = original.next_multipart().unwrap();
    /// assert!(std::matches!(result, Part::Intermediate(1)));
    /// # }
    /// ```
    pub fn next_multipart(self) -> Result<Self, PartError> {
        match self {
            Part::Intermediate(part) => Ok(Part::Intermediate(part + 1)),
            Part::Final(_) => Err(PartError::AlreadyFinishedMultipart),
            Part::Single => Err(PartError::NotMultipart),
        }
    }

    /// Generate new instance keeping the part index but change to final message
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let original = Part::start_multipart();
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    ///
    /// let result = original.as_final_multipart().unwrap();
    /// assert!(std::matches!(result, Part::Final(0)));
    /// # }
    /// ```
    pub fn as_final_multipart(self) -> Result<Self, PartError> {
        match self {
            Part::Intermediate(part) => Ok(Part::Final(part)),
            Part::Final(_) => Err(PartError::AlreadyFinishedMultipart),
            Part::Single => Err(PartError::NotMultipart),
        }
    }

    /// Generate new instance keeping the part index but change to final message
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let original = Part::start_multipart();
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    ///
    /// let result = original.next_final_multipart().unwrap();
    /// assert!(std::matches!(result, Part::Final(1)));
    /// # }
    /// ```
    pub fn next_final_multipart(self) -> Result<Self, PartError> {
        match self {
            Part::Intermediate(part) => Ok(Part::Final(part + 1)),
            Part::Final(_) => Err(PartError::AlreadyFinishedMultipart),
            Part::Single => Err(PartError::NotMultipart),
        }
    }

    /// Update to next index if possible.
    /// This is used to update the part index to the one in other if it is the next consecutive one
    /// ## Examples
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let mut original = Part::start_multipart();
    ///
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    /// original.update_to_next_part(&Part::Intermediate(1)).unwrap();
    ///
    /// assert!(std::matches!(original, Part::Intermediate(1)));
    /// # }
    /// ```
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let mut original = Part::start_multipart();
    ///
    /// assert!(std::matches!(original, Part::Intermediate(0)));
    /// assert!(std::matches!(original.update_to_next_part(&Part::Intermediate(2)),
    ///                       Err(PartError::NotConsecutivePart)));
    /// # }
    /// ```
    /// ```rust
    /// # use rustymq::base::{Part, PartError};
    /// # fn main() {
    /// let mut original = Part::Final(2);
    ///
    /// assert!(std::matches!(original.update_to_next_part(&Part::Intermediate(0)),
    ///                       Err(PartError::AlreadyFinishedMultipart)));
    /// # }
    /// ```
    pub fn update_to_next_part(&mut self, other: &Self) -> Result<(), PartError> {
        match (&self, other) {
            (Part::Intermediate(part), Part::Intermediate(other_part))
                if *part + 1 == *other_part =>
            {
                Ok(*self = other.clone())
            }
            (Part::Intermediate(part), Part::Final(other_part)) if *part + 1 == *other_part => {
                Ok(*self = other.clone())
            }
            (Part::Intermediate(part), Part::Intermediate(other_part))
                if *part + 1 != *other_part =>
            {
                Err(PartError::NotConsecutivePart)
            }
            (Part::Intermediate(part), Part::Final(other_part)) if *part + 1 != *other_part => {
                Err(PartError::NotConsecutivePart)
            }
            (Part::Final(_), _) => Err(PartError::AlreadyFinishedMultipart),
            _ => Err(PartError::NotMultipart),
        }
    }

    /// Generate new instance with next index if possible from other. Same as update_to_next_part function without mutability
    pub fn to_next_part(self, other: &Self) -> Result<Self, PartError> {
        match (self, other) {
            (Part::Intermediate(part), Part::Intermediate(other_part))
                if part + 1 == *other_part =>
            {
                Ok(other.clone())
            }
            (Part::Intermediate(part), Part::Final(other_part)) if part + 1 == *other_part => {
                Ok(other.clone())
            }
            (Part::Intermediate(part), Part::Intermediate(other_part))
                if part + 1 != *other_part =>
            {
                Err(PartError::NotConsecutivePart)
            }
            (Part::Intermediate(part), Part::Final(other_part)) if part + 1 != *other_part => {
                Err(PartError::NotConsecutivePart)
            }
            (Part::Final(_), _) => Err(PartError::AlreadyFinishedMultipart),
            _ => Err(PartError::NotMultipart),
        }
    }
}

/// # Multipart message part tracking error
/// PartError enum to handle errors regarding the tracking of multipart messages
#[derive(Debug)]
pub enum PartError {
    /// Continuation requested of a non-multipart message
    NotMultipart,
    /// Continuation of an already closed or finalized multipart message
    AlreadyFinishedMultipart,
    /// Part not consecutive
    NotConsecutivePart,
}

/// Type definition for message identifier data structure
pub type MessageId = Identifier;
/// Type definition for conversation identifier data structure
pub type ConversationId = Identifier;
/// Type definition for conversation peer identifier data structure
pub type PeerId = Identifier;
/// Type definition for the identifier for the different communication models. For details see [super::super::model]:here
pub type CommunicationModelId = u16;

/// # Metadata for every ZeroMQ Message
/// This datastructure contains data for tracking, and routing messages between links and the built
/// up communication network
/// ## Conained metadata
/// * Randomly generated message indentifier for tracking messages
/// * Randomly generated conversation identifier for tracking conversations
/// * Model ID handled by the communication models used. For defails see [super::super::model]:here
/// * Peer ID for identifying the peer the message is addressed to or received from. Optionality is dependent on the communication model used.
///   For details see [super::super::model]:here
/// * Optional multipart message indexing
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct MessageMetadata {
    /// Message identifier for tracking a message. By default it is randomly generated
    messageid: Option<MessageId>,
    /// Conversation identifier for tracking a conversation. By defailt it is randomly generated
    conversationid: Option<ConversationId>,
    /// Model id for identification of the communication model used. For details see [super::super::model]:here
    modelid: Option<CommunicationModelId>,
    /// Peer id for idetification of the peer the message is addressed to or received from
    peerid: Option<PeerId>,
    /// Part tracker for multipart message tracking
    part: Part,
}

impl MessageMetadata {
    /// Create a new randomly generated MessageMetadata instance
    /// * Message identifier is set to None
    /// * Conversation identifier set to None
    /// * Model identifier set to None
    /// * Peer identifier set to None
    /// * Part tracker set to None
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    ///
    /// assert!(std::matches!(metadata.peer_id(), None));
    /// assert!(std::matches!(metadata.part(), Part::Single));
    /// # }
    /// ```
    pub fn new() -> Self {
        Self {
            messageid: None,
            conversationid: None,
            modelid: None,
            peerid: None,
            part: Part::Single,
        }
    }

    /// Create a new randomly generated MessageMetadata instance for tracking a multipart message
    /// * Message identifier randomly generated
    /// * Conversation identifier randomly generated
    /// * Model identifier set to None
    /// * Peer identifier set to None
    /// * Part tracker set to Part::Intermediate(0)
    /// ## Example
    /// ```rust
    /// # use rustymq::base::MessageMetadata;
    /// # use rustymq::base::Part;
    /// # fn main() {
    /// let metadata = MessageMetadata::new_multipart();
    ///
    /// assert!(std::matches!(metadata.peer_id(), None));
    /// assert!(std::matches!(metadata.part(), Part::Intermediate(0)));
    /// # }
    /// ```
    pub fn new_multipart() -> Self {
        Self {
            part: Part::start_multipart(),
            ..Self::new()
        }
    }

    /// Generate new instance from current one with a message id applied to the message
    ///
    /// This method will commit the message id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId, MessageId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.message_id(), None));
    ///
    /// let message_id = MessageId::new(5);
    /// let metadata = metadata.apply_message_id(message_id.clone());
    /// assert!(std::matches!(metadata.message_id(), Some(message_id)));
    /// # }
    /// ```
    pub fn apply_message_id(self, id: MessageId) -> Self {
        Self {
            messageid: Some(id),
            ..self
        }
    }

    /// Generate new instance from current one with a random message id applied to the message
    ///
    /// This method will commit the message id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.message_id(), None));
    ///
    /// let metadata = metadata.apply_random_message_id();
    /// assert!(std::matches!(metadata.message_id(), Some(_)));
    /// # }
    /// ```
    pub fn apply_random_message_id(self) -> Self {
        Self {
            messageid: Some(MessageId::new_random()),
            ..self
        }
    }

    /// Generate new instance from current one with a random message id if not already set
    ///
    /// This method will commit the message id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.message_id(), None));
    ///
    /// let metadata = metadata.ensure_random_message_id();
    /// assert!(std::matches!(metadata.message_id(), Some(_)));
    /// # }
    /// ```
    pub fn ensure_random_message_id(self) -> Self {
        Self {
            messageid: if let Some(id) = self.messageid {
                Some(id)
            } else {
                Some(MessageId::new_random())
            },
            ..self
        }
    }

    /// Generate new instance from current one with a conversation id applied to the message
    ///
    /// This method will commit the conversation id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId, ConversationId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.conversation_id(), None));
    ///
    /// let conversation_id = ConversationId::new(5);
    /// let metadata = metadata.apply_conversation_id(conversation_id.clone());
    /// assert!(std::matches!(metadata.conversation_id(), Some(conversation_id)));
    /// # }
    /// ```
    pub fn apply_conversation_id(self, id: ConversationId) -> Self {
        Self {
            conversationid: Some(id),
            ..self
        }
    }

    /// Generate new instance from current one with a random conversation id applied to the conversation
    ///
    /// This method will commit the conversation id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.conversation_id(), None));
    ///
    /// let metadata = metadata.apply_random_conversation_id();
    /// assert!(std::matches!(metadata.conversation_id(), Some(_)));
    /// # }
    /// ```
    pub fn apply_random_conversation_id(self) -> Self {
        Self {
            conversationid: Some(ConversationId::new_random()),
            ..self
        }
    }

    /// Generate new instance from current one with a random conversation id if not already set
    ///
    /// This method will commit the conversation id given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.conversation_id(), None));
    ///
    /// let metadata = metadata.ensure_random_conversation_id();
    /// assert!(std::matches!(metadata.conversation_id(), Some(_)));
    /// # }
    /// ```
    pub fn ensure_random_conversation_id(self) -> Self {
        Self {
            conversationid: if let Some(id) = self.conversationid {
                Some(id)
            } else {
                Some(ConversationId::new_random())
            },
            ..self
        }
    }

    /// Generate new instance from current one with a peer identifier applied
    ///
    /// This method will commit the peer identifier given to the metadata but retain all the other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part, PeerId};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.peer_id(), None));
    ///
    /// let peer_id = PeerId::new(5);
    /// let metadata = metadata.apply_peer_id(peer_id.clone());
    /// assert!(std::matches!(metadata.peer_id(), Some(peer_id)));
    /// # }
    /// ```
    pub fn apply_peer_id(self, id: PeerId) -> Self {
        Self {
            peerid: Some(id),
            ..self
        }
    }

    /// Generate new instace with a new communication model id. (Only needed for custom communication model implementation)
    ///
    /// This is only needed in case you are implementing your own communication model. It will retain all fields of the metadata
    /// except for the communication model identifier given as the parameter
    pub fn apply_communication_model_id(self, model_id: CommunicationModelId) -> Self {
        Self {
            modelid: Some(model_id),
            ..self
        }
    }

    /// Generate new instance from current one with multipart message part tracker
    ///
    /// This method will set the part tracking of the metadata to Some(Part::Intermediate(0)) while
    /// retaining all other fields
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let metadata = MessageMetadata::new();
    /// assert!(std::matches!(metadata.part(), Part::Single));
    ///
    /// let metadata = metadata.started_multipart();
    /// assert!(std::matches!(metadata.part(), Part::Intermediate(0)));
    /// # }
    /// ```
    pub fn started_multipart(self) -> Self {
        Self {
            part: Part::start_multipart(),
            ..self
        }
    }

    /// Generate new instance for continuing an already existing conversation
    ///
    /// This method will retain all fields of the metadata except message metadata if already set
    /// and the part is set to single part
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let original = MessageMetadata::new().ensure_random_conversation_id()
    ///                                      .ensure_random_message_id();
    /// let original_converstion_id = original.conversation_id().clone();
    /// let original_clone = original.clone();
    ///
    /// let continuation = original.continue_conversation();
    /// assert_ne!(original_clone.message_id(), continuation.message_id());
    /// assert_eq!(original_clone.conversation_id(), continuation.conversation_id());
    /// assert_eq!(original_clone.peer_id(), continuation.peer_id());
    /// # }
    /// ```
    pub fn continue_conversation(self) -> Self {
        Self {
            messageid: if let Some(_) = self.messageid {
                Some(MessageId::new_random())
            } else {
                None
            },
            part: Part::single(),
            ..self
        }
    }

    /// Qurey the stored message id if stored
    pub fn message_id(&self) -> &Option<MessageId> {
        &self.messageid
    }

    /// Query the stored convesation if stored
    pub fn conversation_id(&self) -> &Option<ConversationId> {
        &self.conversationid
    }

    /// Query the stored communication model id (Only needed for custom communication model implementation)
    pub fn communication_model_id(&self) -> &Option<CommunicationModelId> {
        &self.modelid
    }

    /// Query the stored peer identifier (None if not set)
    pub fn peer_id(&self) -> &Option<PeerId> {
        &self.peerid
    }

    /// Query whether the message is multipart
    pub fn is_multipart(&self) -> bool {
        !std::matches!(self.part, Part::Single)
    }

    /// Query the part tracker (None if not set)
    pub fn part(&self) -> &Part {
        &self.part
    }

    /// Generate new instance for multipart message metadata with advancing the part id by one
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let original = MessageMetadata::new_multipart();
    /// assert!(std::matches!(original.part(), Part::Intermediate(0)));
    ///
    /// let result = original.next_multipart().unwrap();
    /// assert!(std::matches!(result.part(), Part::Intermediate(1)));
    /// # }
    /// ```
    pub fn next_multipart(self) -> Result<Self, PartError> {
        Ok(Self {
            part: self.part.next_multipart()?,
            ..self
        })
    }

    /// Generate new instance for multipart message metadata without advancing the part id and changing it to final
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let original = MessageMetadata::new_multipart();
    /// assert!(std::matches!(original.part(), Part::Intermediate(0)));
    ///
    /// let result = original.as_final_multipart().unwrap();
    /// assert!(std::matches!(result.part(), Part::Final(0)));
    /// # }
    /// ```
    pub fn as_final_multipart(self) -> Result<Self, PartError> {
        Ok(Self {
            part: self.part.as_final_multipart()?,
            ..self
        })
    }

    /// Generate new instance for multipart message metadata with advancing the part id and changing it to final
    /// ## Example
    /// ```rust
    /// # use rustymq::base::{MessageMetadata, Part};
    /// # fn main() {
    /// let original = MessageMetadata::new_multipart();
    /// assert!(std::matches!(original.part(), Part::Intermediate(0)));
    ///
    /// let result = original.as_final_multipart().unwrap();
    /// assert!(std::matches!(result.part(), Part::Final(0)));
    /// # }
    /// ```
    pub fn next_final_multipart(self) -> Result<Self, PartError> {
        Ok(Self {
            part: self.part.next_final_multipart()?,
            ..self
        })
    }
}

/// RustyMQ Message Trait for a universal interface for messages with a given associated payload type
pub trait Message: Sized {
    /// Associated type of the payload type
    type Payload;

    /// Create a new message with a the given payload
    fn new(payload: Self::Payload) -> Self;

    /// Create a new message with the given metadata and payload
    fn with_metadata(metadata: MessageMetadata, payload: Self::Payload) -> Self;

    /// Query the metadata stored in the message
    fn metadata(&self) -> &MessageMetadata;

    /// Consume message and return metadata
    fn into_metadata(self) -> MessageMetadata;

    /// Query the payload of the message
    fn payload(&self) -> &Self::Payload;

    /// Consume message and return payload
    fn into_payload(self) -> Self::Payload;

    /// Consume message and return metadata and payload
    fn into_parts(self) -> (MessageMetadata, Self::Payload);

    /// Generate new instance with mutated metadata but keeping the payload.
    fn mutated_metadata<Mutator: Fn(MessageMetadata) -> MessageMetadata>(
        self,
        mutator: Mutator,
    ) -> Self {
        let (meta, payload) = self.into_parts();
        Self::with_metadata(mutator(meta), payload)
    }

    /// Query the message identifier from the stored metadata
    fn message_id(&self) -> &Option<MessageId> {
        self.metadata().message_id()
    }

    /// Query the conversation identifier from the stored metadata
    fn conversation_id(&self) -> &Option<ConversationId> {
        self.metadata().conversation_id()
    }

    /// Query the communication model identifier from the stored metadata if present
    fn communication_model_id(&self) -> &Option<CommunicationModelId> {
        self.metadata().communication_model_id()
    }

    /// Query the peer identifier from the stored metadata if present
    fn peer_id(&self) -> &Option<PeerId> {
        self.metadata().peer_id()
    }

    /// Query if the stored metadata is multipart
    fn is_multipart(&self) -> bool {
        self.metadata().is_multipart()
    }

    /// Query if part tracker from the stored metadata
    fn part(&self) -> &Part {
        self.metadata().part()
    }

    /// Generate new instance with new message identifier in the stored metadata but keeping the payload
    fn apply_message_id(self, message_id: MessageId) -> Self {
        self.mutated_metadata(|x| x.apply_message_id(message_id))
    }

    /// Generate new instance with new random message identifier in the stored metadata but keeping the payload
    fn apply_random_message_id(self) -> Self {
        self.mutated_metadata(|x| x.apply_random_message_id())
    }

    /// Generate new instance with with ensured random message identifier (kept if already set) in the stored metadata but keeping the payload
    fn ensure_random_message_id(self) -> Self {
        self.mutated_metadata(|x| x.ensure_random_message_id())
    }

    /// Generate new instance with new conversation identifier in the stored metadata but keeping the payload
    fn apply_conversation_id(self, conversation_id: ConversationId) -> Self {
        self.mutated_metadata(|x| x.apply_conversation_id(conversation_id))
    }

    /// Generate new instance with new random conversation identifier in the stored metadata but keeping the payload
    fn apply_random_conversation_id(self) -> Self {
        self.mutated_metadata(|x| x.apply_random_conversation_id())
    }

    /// Generate new instance with with ensured random conversation identifier (kept if already set) in the stored metadata but keeping the payload
    fn ensure_random_conversation_id(self) -> Self {
        self.mutated_metadata(|x| x.ensure_random_conversation_id())
    }

    /// Generate new instance with new peer identifier in the stored metadata but keeping the payload
    fn apply_peer_id(self, peer_id: PeerId) -> Self {
        self.mutated_metadata(|x| x.apply_peer_id(peer_id))
    }

    /// Generate new instance with new communication model identifier in metadata but keeping everything else
    fn apply_communication_model_id(self, model_id: CommunicationModelId) -> Self {
        self.mutated_metadata(|metadata| metadata.apply_communication_model_id(model_id))
    }

    /// Generate new instance by continuing metadata exchange and with the given payload
    fn continue_conversation(self, payload: Self::Payload) -> Self
    where
        Self: Sized,
    {
        let metadata = self.into_metadata();
        Self::with_metadata(metadata.continue_conversation(), payload)
    }

    /// Generate new instance by keeping payload and continuing metadata exchange
    fn continue_conversation_from_metadata(self, metadata: MessageMetadata) -> Self
    where
        Self: Sized,
    {
        Self::with_metadata(metadata.continue_conversation(), self.into_payload())
    }
}

/// RawMessage is a message type implementing the Message trait, with a binary buffer set as payload
#[derive(Clone, Debug)]
pub struct RawMessage {
    meta: MessageMetadata,
    payload: Buffer,
}

impl Message for RawMessage {
    type Payload = Buffer;

    fn new(payload: Self::Payload) -> Self {
        Self {
            meta: MessageMetadata::new(),
            payload: payload,
        }
    }

    fn with_metadata(meta: MessageMetadata, payload: Self::Payload) -> Self {
        Self {
            meta: meta,
            payload: payload,
        }
    }

    fn metadata(&self) -> &MessageMetadata {
        &self.meta
    }

    fn into_metadata(self) -> MessageMetadata {
        self.meta
    }

    fn payload(&self) -> &Buffer {
        &self.payload
    }

    fn into_payload(self) -> Buffer {
        self.payload
    }

    fn into_parts(self) -> (MessageMetadata, Buffer) {
        (self.meta, self.payload)
    }
}

/// Implementing this trait on a struct allows for using a [`TypedMessage`] with the struct
pub trait SerializableMessagePayload: Sized {
    /// Associated type for the possible error during serialization
    type SerializationError: std::fmt::Debug;
    /// Associated type for the possible error during deserialization
    type DeserializationError: std::fmt::Debug;

    /// Serialize Self into a binary buffer
    fn serialize(self) -> Result<Buffer, Self::SerializationError>;
    /// Deserialize a binary buffer into Self
    fn deserialize<'a>(buffer: BufferSlice<'a>) -> Result<Self, Self::DeserializationError>;
}

/// Blanket implementation for all [`Serializable`] traits to be used as a SerializableMessagePayload using [`FlatSerializer`]
impl<T> SerializableMessagePayload for T
where
    T: Serializable,
{
    type SerializationError = ();
    type DeserializationError = serializer::Error;

    fn serialize(self) -> Result<Buffer, Self::SerializationError> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize_pass(self);
        Ok(serializer.finalize())
    }

    fn deserialize<'a>(buffer: BufferSlice<'a>) -> Result<Self, Self::DeserializationError> {
        let mut deserializer = FlatDeserializer::new(buffer)?;
        Ok(T::deserialize(&mut deserializer)?)
    }
}

/// Typed message is a generic implementation for a Message trait with custom type
#[derive(Clone)]
pub struct TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    meta: MessageMetadata,
    payload: T,
}

impl<T> TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    pub fn mutated_payload(self, mutator: &dyn Fn(T) -> T) -> Self {
        Self {
            payload: mutator(self.payload),
            ..self
        }
    }
}

impl<T> Message for TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    type Payload = T;

    fn new(payload: Self::Payload) -> Self {
        Self {
            meta: MessageMetadata::new(),
            payload: payload,
        }
    }

    fn with_metadata(meta: MessageMetadata, payload: Self::Payload) -> Self {
        Self {
            meta: meta,
            payload: payload,
        }
    }

    fn metadata(&self) -> &MessageMetadata {
        &self.meta
    }

    fn into_metadata(self) -> MessageMetadata {
        self.meta
    }

    fn payload(&self) -> &T {
        &self.payload
    }

    fn into_payload(self) -> T {
        self.payload
    }

    fn into_parts(self) -> (MessageMetadata, T) {
        (self.meta, self.payload)
    }
}

impl<T> TryFrom<TypedMessage<T>> for RawMessage
where
    T: SerializableMessagePayload,
{
    type Error = T::SerializationError;

    fn try_from(value: TypedMessage<T>) -> Result<Self, Self::Error> {
        let (meta, payload) = value.into_parts();
        Ok(Self {
            meta: meta,
            payload: payload.serialize()?,
        })
    }
}

impl<T> TryFrom<RawMessage> for TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    type Error = T::DeserializationError;

    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let (meta, payload) = value.into_parts();
        Ok(Self {
            meta: meta,
            payload: T::deserialize(&payload)?,
        })
    }
}

impl<T> Deref for TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> DerefMut for TypedMessage<T>
where
    T: SerializableMessagePayload,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

impl Serializable for Part {
    #[inline]
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        match self {
            Part::Single => serializer.serialize(&0u8),
            Part::Intermediate(value) => {
                serializer.serialize(&1u8);
                serializer.serialize(value)
            }
            Part::Final(value) => {
                serializer.serialize(&2u8);
                serializer.serialize(value)
            }
        }
    }

    #[inline]
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        match deserializer.deserialize::<u8>()? {
            0 => Ok(Part::Single),
            1 => Ok(Part::Intermediate(deserializer.deserialize()?)),
            2 => Ok(Part::Final(deserializer.deserialize()?)),
            _ => Err(serializer::Error::DemarshallingFailed),
        }
    }
}

impl Serializable for MessageMetadata {
    #[inline]
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&MESSAGE_METADATA_VERSION);
        serializer.serialize(&self.messageid);
        serializer.serialize(&self.conversationid);
        serializer.serialize(&self.modelid);
        serializer.serialize(&self.part);
    }

    #[inline]
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        if deserializer.deserialize::<u8>()? != MESSAGE_METADATA_VERSION {
            panic!("Incorrect message metadata version");
        }
        Ok(Self {
            messageid: deserializer.deserialize()?,
            conversationid: deserializer.deserialize()?,
            modelid: deserializer.deserialize()?,
            peerid: None,
            part: deserializer.deserialize()?,
        })
    }
}

impl Serializable for RawMessage {
    #[inline]
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.meta);
        serializer.serialize_raw_slice(self.payload.as_slice());
    }

    #[inline]
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self {
            meta: deserializer.deserialize()?,
            payload: deserializer.deserialize_raw_slice()?,
        })
    }
}
