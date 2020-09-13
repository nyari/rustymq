use std::ops::{Deref, DerefMut, Range};

#[derive(Debug)]
pub enum FieldedBufferError
{
    FieldIndexOutOfBounds
}

pub type FieldedBufferResult<T> = Result<T, FieldedBufferError>;

pub trait Buffer<T> {
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;

    fn as_slice(&self) -> &[T];
    fn as_mut_slice(&mut self) -> &mut [T];
}

pub trait ExtendFromSlice<T>
    where T:Clone
{
    fn extend_from_slice(&mut self, other: &[T]);
}

struct BufferFieldData {
    offsets: Vec<usize>,
}

impl BufferFieldData {
    pub fn new() -> Self {
        Self { 
            offsets: vec![0],
        }
    }

    pub fn commit(&mut self, offset:usize) {
        if self.last_offset() != offset {
            self.offsets.push(offset);
        }
    }

    pub fn count(&self) -> usize {
        self.offsets.len()
    }

    pub fn range(&self, index: usize, end_offset:usize) -> FieldedBufferResult<Range<usize>> 
    {
        let result_begin_offset = self.offsets.get(index).ok_or(FieldedBufferError::FieldIndexOutOfBounds)?;
        let result_end_offset = self.offsets.get(index + 1).unwrap_or(&end_offset);
        Ok(Range{start: result_begin_offset.clone(), end: result_end_offset.clone()})
    }

    pub fn last_offset(&self) -> usize {
        self.offsets.last().unwrap().clone()
    }

    pub fn field_sizes(&self) -> Vec<usize> {
        self.offsets.windows(2)
                    .map(|x| x.iter()
                              .rfold(0, |acc, x| if acc == 0 {x.clone()} else {acc - x}))
                    .collect()
    }
}

pub struct FieldedBuffer<T>
{
    raw: Vec<T>,
    fields: BufferFieldData
}

impl<T> FieldedBuffer<T> {
    pub fn new() -> Self {
        Self {
            raw: Vec::new(),
            fields: BufferFieldData::new()
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            raw: Vec::with_capacity(capacity),
            fields: BufferFieldData::new()
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.raw.reserve(additional)
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        self.raw.reserve_exact(additional)
    }

    pub fn shrink_to_fit(&mut self) {
        self.raw.shrink_to_fit()
    }

    pub fn field_count(&self) -> usize {
        self.fields.count()
    }

    pub fn field_as_slice(&self, index: usize) -> FieldedBufferResult<&[T]> {
        let range = self.fields.range(index, self.raw.len())?;
        Ok(self.raw.get(range).expect("FieldedBuffer internal error"))
    }
    
    pub fn field_as_mut_slice(&mut self, index: usize) -> FieldedBufferResult<&mut [T]> {
        let range = self.fields.range(index, self.raw.len())?;
        Ok(self.raw.get_mut(range).expect("FieldedBuffer internal error"))
    }

    pub fn field_sizes(&self) -> Vec<usize> {
        let mut result = self.fields.field_sizes();
        result.push(self.len());
        result
    }

    pub fn commit_field(&mut self) {
        self.fields.commit(self.raw.len())
    }

    pub fn full_as_slice(&self) -> &[T] {
        self.raw.as_slice()
    }

    pub fn full_as_mut_slice(&mut self) -> &mut [T] {
        self.raw.as_mut_slice()
    }

    pub fn finalize(self) -> Vec<T> {
        self.raw
    }

    pub fn append(&mut self, other:&mut Vec<T>) {
        self.raw.append(other)
    }

    pub fn append_consume(&mut self, mut other: Vec<T>) {
        self.append(&mut other)
    }

    pub fn push(&mut self, value: T) {
        self.raw.push(value)
    }
}

impl<U> Extend<U> for FieldedBuffer<U>
{
    fn extend<T: IntoIterator<Item=U>>(&mut self, iter: T) {
        self.raw.extend(iter)
    }
}

impl<T> ExtendFromSlice<T> for FieldedBuffer<T>
    where T: Clone
{
    fn extend_from_slice(&mut self, other: &[T]) {
        self.raw.extend_from_slice(other)
    }
}

impl<T> Buffer<T> for FieldedBuffer<T>
{
    fn capacity(&self) -> usize {
        self.raw.capacity() - self.fields.last_offset()
    }

    fn len(&self) -> usize {
        self.raw.len() - self.fields.last_offset()
    }

    fn as_slice(&self) -> &[T] {
        self.field_as_slice(self.field_count() - 1).unwrap()
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        self.field_as_mut_slice(self.field_count() - 1).unwrap()
    }
}

impl<T> Deref for dyn Buffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> DerefMut for dyn Buffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}
