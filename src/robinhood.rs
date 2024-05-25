use ahash::{AHasher, RandomState};
use std::hash::{BuildHasher, Hasher};

pub struct Probe {
    // whether the key was contained.
    pub contained: bool,
    // number of probes needed.
    pub psl: usize,
}

// record of an update procedure.
pub struct Update {
    // the number of probes made, in total.
    pub total_probes: usize,
    // the number of writes made, in total.
    // the number of keys which were moved by "robin hood" is equal to this minus 1.
    pub total_writes: usize,
}

// dummy hash-set for u64 keys.
//
// implements robin-hood-hashing with backward-shift deletion
pub struct RobinHood {
    hasher: RandomState,
    buckets: Vec<Option<u64>>,
    len: usize,
}

impl RobinHood {
    pub fn new(capacity: usize) -> Self {
        RobinHood {
            hasher: RandomState::new(),
            buckets: vec![None; capacity],
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn bucket_for(&self, key: u64) -> usize {
        (self.hasher.hash_one(key) % (self.buckets.len() as u64)) as usize
    }

    pub fn load_factor(&self) -> f64 {
        (self.len as f64) / (self.buckets.len() as f64)
    }

    pub fn probe(&self, key: u64) -> Probe {
        let mut psl = 1;
        let mut bucket = self.bucket_for(key);
        loop {
            match self.buckets[bucket] {
                None => {
                    return Probe {
                        contained: false,
                        psl,
                    }
                }
                Some(k) if k == key => {
                    return Probe {
                        contained: true,
                        psl,
                    }
                }
                Some(k) => {
                    if self.psl_of(k, bucket) < psl {
                        return Probe {
                            contained: false,
                            psl,
                        };
                    }
                }
            }

            psl += 1;
            bucket = (bucket + 1) % self.buckets.len()
        }
    }

    fn psl_of(&self, key: u64, bucket: usize) -> usize {
        let home = self.bucket_for(key);
        1 + if bucket < home {
            (bucket + self.buckets.len()) - home
        } else {
            bucket - home
        }
    }

    // insert a key
    pub fn insert(&mut self, key: u64) -> Update {
        let mut update = Update {
            total_probes: 1,
            total_writes: 1,
        };

        let mut home_bucket = self.bucket_for(key);
        let mut active_key = key;
        let mut psl = 1;
        self.len += 1;
        loop {
            let bucket = (home_bucket + psl - 1) % self.buckets.len();

            if self.buckets[bucket].is_none() {
                self.buckets[bucket] = Some(active_key);
                return update;
            }
            let contained_key = self.buckets[bucket].unwrap();
            if contained_key == active_key {
                if active_key == key {
                    self.len -= 1;
                }
                return update;
            }

            let contained_home = self.bucket_for(contained_key);
            let contained_psl = self.psl_of(contained_key, bucket);

            if contained_psl < psl {
                self.buckets[bucket] = Some(active_key);
                home_bucket = contained_home;
                active_key = contained_key;
                psl = contained_psl;
                update.total_writes += 1;
            }

            psl += 1;
            update.total_probes += 1;
        }
    }

    // remove a key from the map.
    pub fn remove(&mut self, key: u64) -> Update {
        let probe = self.probe(key);
        let mut update = Update {
            total_probes: probe.psl,
            total_writes: 0,
        };

        if !probe.contained {
            return update;
        }

        self.len -= 1;

        let mut bucket = (self.bucket_for(key) + probe.psl - 1) % self.buckets.len();
        self.buckets[bucket] = None;
        update.total_writes += 1;
        update.total_probes += 1;

        loop {
            let next_bucket = (bucket + 1) % self.buckets.len();
            let shift_key = match self.buckets[next_bucket] {
                None => return update,
                Some(k) if self.psl_of(k, next_bucket) == 1 => return update,
                Some(k) => k,
            };

            self.buckets[bucket] = Some(shift_key);
            bucket = next_bucket;
            update.total_writes += 1;
            update.total_probes += 1;
        }
    }
}
