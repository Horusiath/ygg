use rand::{Rng, RngCore};

pub trait RngExt: RngCore {
    fn shuffle<V>(&mut self, values: &mut [V]) {
        values.sort_by(|_, _| self.gen::<i32>().cmp(&0))
    }
}

impl<T> RngExt for T where T: RngCore {}
