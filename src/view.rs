use rand::{Rng, RngCore};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct View<V>(Vec<V>);

impl<V> View<V>
where
    V: Eq,
{
    pub fn new(capacity: usize) -> Self {
        View(Vec::with_capacity(capacity))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains(&self, value: &V) -> bool {
        for v in self.0.iter() {
            if v == value {
                return true;
            }
        }
        false
    }

    pub fn remove(&mut self, value: &V) -> Option<V> {
        let idx = self.0.iter().position(|v| v.eq(value))?;
        let removed = self.0.remove(idx);
        Some(removed)
    }

    pub fn remove_at(&mut self, at: usize) -> Option<V> {
        if self.0.is_empty() {
            None
        } else {
            let removed = self.0.remove(at % self.len());
            Some(removed)
        }
    }

    pub fn insert_replace<R: RngCore>(&mut self, value: V, rng: &mut R) -> Option<V> {
        if self.is_full() {
            let idx = rng.gen_range(0..self.len());
            Some(std::mem::replace(&mut self.0[idx], value))
        } else {
            self.0.push(value);
            None
        }
    }

    pub fn iter(&self) -> Iter<V> {
        Iter(self.0.iter())
    }

    pub fn peek(&self, at: usize) -> Option<&V> {
        self.0.get(at % self.0.len())
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct Iter<'a, V>(std::slice::Iter<'a, V>);

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = &'a V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a, V> ExactSizeIterator for Iter<'a, V> {
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }
}
