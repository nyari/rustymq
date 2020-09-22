use std;
use std::convert::{TryInto, TryFrom};
use std::ops::{Deref, DerefMut};
use core::serializer::{Serializable, Serializer, Deserializer};
use core::serializer;

use core::util::Identifier;
pub use core::serializer::Buffer;

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
#[derive(Eq)]
#[derive(Hash)]
pub enum Part {
    Intermediate(u32),
    Final(u32),
    Closing(u32)
}

#[derive(Debug)]
pub enum Error {
    NotMultipart,
    AlreadyFinishedMultipart
}

pub type MessageId = Identifier;
pub type ConversationId = Identifier;
pub type PeerId = Identifier;

#[derive(Clone)]
#[derive(PartialEq)]
#[derive(Eq)]
#[derive(Hash)]
#[derive(Debug)]
pub struct MessageMetadata {
    messageid: MessageId,
    conversationid: ConversationId,
    peerid: Option<PeerId>,
    part: Option<Part>
}

impl MessageMetadata {
    pub fn new() -> Self {
        Self {
            messageid: MessageId::new_random(),
            conversationid: ConversationId::new_random(),
            peerid: None,
            part: None
        }
    }

    pub fn new_multipart() -> Self {
        Self {
            part:Some(Part::Intermediate(0)),
            ..Self::new()
        }
    }

    pub fn applied_peer_id(self, id:PeerId) -> Self {
        Self {
            peerid:Some(id),
            ..self
        }
    }

    pub fn started_multipart(self) -> Self {
        Self {
            part: Some(Part::Intermediate(0)),
            ..self
        }
    }

    pub fn continue_exhange(self) -> Self {
        if !self.is_multipart() {
            Self {
                messageid: MessageId::new_random(),
                part: None,
                ..self
            }
        } else {
            Self {
                messageid: MessageId::new_random(),
                ..self
            }
        }
    }

    pub fn message_id(&self) -> &MessageId {
        &self.messageid
    }

    pub fn conversation_id(&self) -> &ConversationId {
        &self.conversationid
    }

    pub fn peer_id(&self) -> &Option<PeerId> {
        &self.peerid
    }

    pub fn is_multipart(&self) -> bool {
        self.part.is_none()
    }

    pub fn part(&self) -> &Option<Part> {
        &self.part
    }

    pub fn next_multipart(self) -> Result<Self, Error> {
        match self.part {
            Some(Part::Intermediate(part)) => Ok(Self {
                part: Some(Part::Intermediate(part + 1)),
                ..self
            }),
            Some(_) => Err(Error::AlreadyFinishedMultipart),
            None => Err(Error::NotMultipart)
        }
    }

    pub fn as_final_multipart(self) -> Result<Self, Error> {
        match self.part {
            Some(Part::Intermediate(part)) => Ok(Self {
                part: Some(Part::Final(part)),
                ..self
            }),
            Some(_) => Err(Error::AlreadyFinishedMultipart),
            None => Err(Error::NotMultipart)
        }
    }

    pub fn final_multipart(self) -> Result<Self, Error> {
        match self.part {
            Some(Part::Intermediate(part)) => Ok(Self {
                part: Some(Part::Final(part + 1)),
                ..self
            }),
            Some(_) => Err(Error::AlreadyFinishedMultipart),
            None => Err(Error::NotMultipart)
        }
    }

    pub fn close_multipart(self) -> Result<Self, Error> {
        match self.part {
            Some(Part::Intermediate(part)) => Ok(Self {
                part: Some(Part::Closing(part)),
                ..self
            }),
            Some(_) => Err(Error::AlreadyFinishedMultipart),
            None => Err(Error::NotMultipart)
        }
    }
}

pub trait Message: Sized
{
    type Payload;

    fn new(payload: Self::Payload) -> Self;
    fn with_metadata(meta: MessageMetadata, payload: Self::Payload) -> Self;

    fn metadata(&self) -> &MessageMetadata;
    fn yield_metadata(&mut self) -> MessageMetadata;
    fn into_metadata(self) -> MessageMetadata;
    fn mutated_metadata<Mutator: Fn(MessageMetadata) -> MessageMetadata>(self, mutator: Mutator) -> Self {
        let (meta, payload) = self.into_parts();
        Self::with_metadata(mutator(meta), payload)
    }

    fn apply_peer_id(self, peer_id: PeerId) -> Self {
        self.mutated_metadata(|x| x.applied_peer_id(peer_id))
    }

    fn message_id(&self) -> &MessageId {
        self.metadata().message_id()
    }

    fn conversation_id(&self) -> &ConversationId {
        self.metadata().conversation_id()
    }

    fn peer_id(&self) -> &Option<PeerId> {
        self.metadata().peer_id()
    }

    fn is_multipart(&self) -> bool {
        self.metadata().is_multipart()
    }

    fn part(&self) -> &Option<Part> {
        self.metadata().part()
    }

    fn payload(&self) -> &Self::Payload;
    fn into_payload(self) -> Self::Payload;

    fn into_parts(self) -> (MessageMetadata, Self::Payload);

    fn continue_exhange(self, payload: Self::Payload) -> Self where Self: Sized {
        Self::with_metadata(self.metadata()
                                .clone()
                                .continue_exhange(),
                            payload)
    }

    fn continue_exchange_metadata(self, meta: MessageMetadata) -> Self where Self: Sized {
        Self::with_metadata(meta.continue_exhange(),
                            self.into_payload())
    }

    fn multipart(meta: &mut MessageMetadata, payload: Self::Payload) -> Self where Self: Sized {
        let original_metadata = meta.clone();
        let result = Self::with_metadata(original_metadata, payload);
        *meta = meta.clone().next_multipart().unwrap();
        result
    }

    fn into_final_multipart(self) -> Self where Self: Sized {
        let (meta, payload) = self.into_parts();
        Self::with_metadata(meta.as_final_multipart().unwrap(),
                            payload)
    }
}

#[derive(Clone)]
#[derive(Debug)]
pub struct RawMessage
{
    meta: MessageMetadata,
    payload: Buffer
}

impl Message for RawMessage {
    type Payload = Buffer;

    fn new(payload: Self::Payload) -> Self {
        Self {
            meta: MessageMetadata::new(),
            payload: payload
        }
    }

    fn with_metadata(meta: MessageMetadata, payload: Self::Payload) -> Self {
        Self {
            meta: meta,
            payload: payload
        }
    }

    fn metadata(&self) -> &MessageMetadata {
        &self.meta
    }

    fn yield_metadata(&mut self) -> MessageMetadata {
        std::mem::replace(&mut self.meta, MessageMetadata::new())
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

#[derive(Clone)]
pub struct TypedMessage<T>
    where T: TryInto<Buffer>, Buffer: TryInto<T>
{
    meta: MessageMetadata,
    payload: T
}

impl<T> TypedMessage<T>
    where T: TryInto<Buffer>, Buffer: TryInto<T>
{
    pub fn mutated_payload(self, mutator: &dyn Fn(T) -> T) -> Self {
        Self {
            payload: mutator(self.payload),
            ..self
        }
    }
}

impl<T> Message for TypedMessage<T>
    where T: TryInto<Buffer>, Buffer: TryInto<T>
{
    type Payload = T;

    fn new(payload: Self::Payload) -> Self {
        Self {
            meta: MessageMetadata::new(),
            payload: payload
        }
    }

    fn with_metadata(meta: MessageMetadata, payload: Self::Payload) -> Self {
        Self {
            meta: meta,
            payload: payload
        }
    }

    fn metadata(&self) -> &MessageMetadata {
        &self.meta
    }

    fn yield_metadata(&mut self) -> MessageMetadata {
        std::mem::replace(&mut self.meta, MessageMetadata::new())
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
    where T: TryInto<Buffer>, Buffer: TryInto<T>, <T as TryInto<Buffer>>::Error : std::fmt::Debug
{
    type Error = <T as TryInto<Buffer>>::Error;

    fn try_from(value: TypedMessage<T>) -> Result<Self, Self::Error> {
        let meta = value.metadata().clone();
        let payload: Buffer = value.into_payload().try_into()?;
        Ok(Self {
            meta: meta,
            payload: payload
        })
    }
}

impl<T> TryFrom<RawMessage> for TypedMessage<T>
    where T: TryInto<Buffer>, Buffer: TryInto<T>, <Buffer as TryInto<T>>::Error : std::fmt::Debug
{
    type Error = <Buffer as TryInto<T>>::Error;

    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let meta = value.metadata().clone();
        let payload: T = value.into_payload().try_into()?;
        Ok(Self {
            meta: meta,
            payload: payload
        })
    }
}

impl<T> Deref for TypedMessage<T>
    where T: TryInto<Buffer> + TryFrom<Buffer>
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> DerefMut for TypedMessage<T> 
    where T: TryInto<Buffer> + TryFrom<Buffer>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

impl Serializable for Part {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        match self {
            Part::Intermediate(value) => {serializer.serialize(&0u8); serializer.serialize(value)},
            Part::Final(value) => {serializer.serialize(&1u8); serializer.serialize(value)},
            Part::Closing(value) => {serializer.serialize(&2u8); serializer.serialize(value)}
        }
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        match deserializer.deserialize::<u8>()? {
            0 => Ok(Part::Intermediate(deserializer.deserialize()?)),
            1 => Ok(Part::Final(deserializer.deserialize()?)),
            2 => Ok(Part::Closing(deserializer.deserialize()?)),
            _ => Err(serializer::Error::DemarshallingFailed)
        }
    }
}

impl Serializable for MessageMetadata {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.messageid);
        serializer.serialize(&self.conversationid);
        serializer.serialize(&self.peerid);
        serializer.serialize(&self.part);
    }
    
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self {
            messageid: deserializer.deserialize()?,
            conversationid: deserializer.deserialize()?,
            peerid: deserializer.deserialize()?,
            part: deserializer.deserialize()?
        })
    }
}

impl Serializable for RawMessage {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.meta);
        serializer.serialize_raw_slice(self.payload.as_slice());
    }
    
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self {
            meta: deserializer.deserialize()?,
            payload: deserializer.deserialize_raw_slice()?
        })
    }
}