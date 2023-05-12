use crate::peer::PeerId;
use rand::{Rng, RngCore};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct View<V>(Vec<(PeerId, V)>);

impl<V> View<V> {
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

    pub fn get(&self, peer: &PeerId) -> Option<&V> {
        for (pid, value) in self.0.iter() {
            if pid == peer {
                return Some(value);
            }
        }
        None
    }

    pub fn get_mut(&mut self, peer: &PeerId) -> Option<&mut V> {
        for (pid, value) in self.0.iter_mut() {
            if pid == peer {
                return Some(value);
            }
        }
        None
    }

    pub fn contains(&self, peer: &PeerId) -> bool {
        for (pid, _) in self.0.iter() {
            if pid == peer {
                return true;
            }
        }
        false
    }

    pub fn remove(&mut self, peer: &PeerId) -> Option<V> {
        let idx = self.0.iter().position(|(pid, _)| pid.eq(peer))?;
        let (_, removed) = self.0.remove(idx);
        Some(removed)
    }

    pub fn remove_at(&mut self, at: usize) -> Option<(PeerId, V)> {
        if self.0.is_empty() {
            None
        } else {
            let removed = self.0.remove(at % self.len());
            Some(removed)
        }
    }

    pub fn insert_replace<R: RngCore>(
        &mut self,
        peer: PeerId,
        value: V,
        rng: &mut R,
    ) -> Option<(PeerId, V)> {
        if self.is_full() {
            let idx = rng.gen_range(0..self.len());
            Some(std::mem::replace(&mut self.0[idx], (peer, value)))
        } else {
            self.0.push((peer, value));
            None
        }
    }

    pub fn iter(&self) -> Iter<V> {
        Iter(self.0.iter())
    }

    pub fn values_mut(&mut self) -> ValuesMut<V> {
        ValuesMut(self.0.iter_mut())
    }

    pub fn keys(&self) -> Keys<V> {
        Keys(self.0.iter())
    }

    pub fn peek_key(&self, at: usize) -> Option<&PeerId> {
        if self.len() == 0 {
            None
        } else {
            let mut i = at % self.len();
            let mut iter = self.keys();
            let mut last = None;
            while i != 0 {
                i -= 1;
                if let Some(v) = iter.next() {
                    last = Some(v);
                } else {
                    break;
                }
            }
            last
        }
    }

    pub fn peek_value_mut<F>(&mut self, at: usize, filter: F) -> Option<&mut V>
    where
        F: Fn(&V) -> bool,
    {
        if self.len() == 0 {
            None
        } else {
            let mut i = at % self.len();
            let mut iter = self.values_mut();
            let mut last = None;
            while i != 0 {
                i -= 1;
                if let Some(v) = iter.next() {
                    if !filter(v) {
                        continue; // skip over
                    }
                    last = Some(v);
                } else {
                    break;
                }
            }
            last
        }
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct Keys<'a, V>(std::slice::Iter<'a, (PeerId, V)>);

impl<'a, V> Iterator for Keys<'a, V> {
    type Item = &'a PeerId;

    fn next(&mut self) -> Option<Self::Item> {
        let (pid, _) = self.0.next()?;
        Some(pid)
    }
}

impl<'a, V> ExactSizeIterator for Keys<'a, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct Iter<'a, V>(std::slice::Iter<'a, (PeerId, V)>);

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (&'a PeerId, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (pid, value) = self.0.next()?;
        Some((pid, value))
    }
}

impl<'a, V> ExactSizeIterator for Iter<'a, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct ValuesMut<'a, V>(std::slice::IterMut<'a, (PeerId, V)>);

impl<'a, V> Iterator for ValuesMut<'a, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, value) = self.0.next()?;
        Some(value)
    }
}

impl<'a, V> ExactSizeIterator for ValuesMut<'a, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}
