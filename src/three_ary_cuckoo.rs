use crate::meta_map::{MetaMap, Metadata};
use crate::{Map, Probe, Update};
use ahash::RandomState;
use rand::prelude::*;

// extra hashers for avoiding collisions.
const HASHER_COUNT: usize = 6;

// dummy hash-set for u64 keys.
//
// implements 3-ary cuckoo hashing.
pub struct ThreeAryCuckoo {
    hashers: Vec<RandomState>,
    buckets: Vec<Option<u64>>,
    meta: MetaMap,
    len: usize,
}

impl ThreeAryCuckoo {
    pub fn new(capacity: usize, meta_bits: usize) -> Self {
        ThreeAryCuckoo {
            hashers: (0..HASHER_COUNT).map(|_| RandomState::new()).collect(),
            buckets: vec![None; capacity],
            meta: MetaMap::new(capacity, meta_bits),
            len: 0,
        }
    }

    // (hash, [bucket_a, bucket_b, bucket_c])
    fn buckets(&self, key: u64) -> (u64, [usize; 3]) {
        let hash_a = self.hashers[0].hash_one(key);
        let h = |h_i: usize| (self.hashers[h_i].hash_one(key) % self.buckets.len() as u64) as usize;

        let bucket_a = h(0);
        let mut bucket_b = h(1);
        let mut bucket_c = h(2);

        // resolve collisions by re-hashing.
        let mut hasher_index = 3;

        while bucket_b == bucket_a {
            bucket_b = h(hasher_index);
            hasher_index += 1;
        }

        while bucket_c == bucket_a || bucket_c == bucket_b {
            bucket_c = h(hasher_index);
            hasher_index += 1; 
        }

        (hash_a, [bucket_a, bucket_b, bucket_c])
    }

    fn set_bucket(&mut self, bucket: usize, key: u64, hash: u64) {
        self.buckets[bucket] = Some(key);
        self.meta.set_full(bucket, Metadata::Hash(hash));
    }

    fn clear_bucket(&mut self, bucket: usize) {
        self.buckets[bucket] = None;
        self.meta.set_empty(bucket);
    }
}

impl Map for ThreeAryCuckoo {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn probe(&self, key: u64) -> Probe {
        let (hash, [bucket_a, bucket_b, bucket_c]) = self.buckets(key);

        let mut probes = 0;

        if !self.meta.hint_not_match(bucket_a, hash) {
            probes += 1;
            if self.buckets[bucket_a] == Some(key) {
                return Probe {
                    contained: true,
                    probes,
                };
            }
        }

        if !self.meta.hint_not_match(bucket_b, hash) {
            probes += 1;
            if self.buckets[bucket_b] == Some(key) {
                return Probe {
                    contained: true,
                    probes,
                };
            }
        }

        if !self.meta.hint_not_match(bucket_c, hash) {
            probes += 1;
            if self.buckets[bucket_c] == Some(key) {
                return Probe {
                    contained: true,
                    probes,
                };
            }
        }

        Probe {
            contained: false,
            probes,
        }
    }

    fn insert(&mut self, key: u64) -> Update {
        const MAX_CHAIN: usize = 128;

        let mut update = Update {
            total_probes: 0,
            total_writes: 1,
            completed: true,
        };

        let mut active_key = key;
        let mut key_info = self.buckets(key);

        // test for presence.
        {
            let (hash, [bucket_a, bucket_b, bucket_c]) = key_info;

            if !self.meta.hint_not_match(bucket_a, hash) {
                update.total_probes += 1;
                if self.buckets[bucket_b] == Some(key) {
                    return update;
                }
            }

            if !self.meta.hint_not_match(bucket_b, hash) {
                update.total_probes += 1;
                if self.buckets[bucket_b] == Some(key) {
                    return update;
                }
            }

            if !self.meta.hint_not_match(bucket_c, hash) {
                update.total_probes += 1;
                if self.buckets[bucket_c] == Some(key) {
                    return update;
                }
            }
        }

        self.len += 1;

        let mut buckets_to_use = [true, true, true];

        // all targets full. evict randomly.
        for _ in 0..MAX_CHAIN {
            let (hash, buckets) = key_info;

            let bucket_indices: Vec<_> = buckets_to_use
                .clone()
                .iter()
                .enumerate()
                .filter_map(|(i, should_use)| should_use.then(|| buckets[i]))
                .collect();

            // if there is an empty bucket, use that.
            for &bucket_index in &bucket_indices {
                if self.meta.hint_empty(bucket_index) {
                    if active_key != key {
                        update.total_writes += 1;
                    }
                    self.set_bucket(bucket_index, active_key, hash);
                    return update;
                } else if self.meta.bits() == 0 {
                    update.total_probes += 1;
                    if self.buckets[bucket_index].is_none() {
                        if active_key != key {
                            update.total_writes += 1;
                        }
                        self.set_bucket(bucket_index, active_key, hash);
                        return update;
                    }
                }
            }

            // no bucket is empty. choose one at random.
            let evict_bucket = loop {
                let evict = rand::thread_rng().gen_range(0..3);
                if buckets_to_use[evict] {
                    break buckets[evict];
                }
            };

            // in this case we've already probed all 3 buckets and don't double count
            if self.meta.bits() > 0 {
                update.total_probes += 1;
            }

            let swap_key = self.buckets[evict_bucket].unwrap();
            update.total_writes += 1;
            self.set_bucket(evict_bucket, active_key, hash);

            key_info = self.buckets(swap_key);

            // the index of this bucket, as seen from the swapped key.
            buckets_to_use = if evict_bucket == key_info.1[0] {
                [false, true, true]
            } else if evict_bucket == key_info.1[1] {
                [true, false, true]
            } else {
                [true, true, false]
            };

            active_key = swap_key;
        }

        update.completed = false;
        update
    }

    fn remove(&mut self, key: u64) -> Update {
        let (hash, [bucket_a, bucket_b, bucket_c]) = self.buckets(key);

        let mut update = Update {
            total_probes: 0,
            total_writes: 0,
            completed: true,
        };

        if !self.meta.hint_not_match(bucket_a, hash) {
            update.total_probes += 1;
            if self.buckets[bucket_a] == Some(key) {
                self.clear_bucket(bucket_a);
                update.total_writes += 1;
                return update;
            }
        }

        if !self.meta.hint_not_match(bucket_b, hash) {
            update.total_probes += 1;
            if self.buckets[bucket_b] == Some(key) {
                self.clear_bucket(bucket_b);
                update.total_writes += 1;
                return update;
            }
        }

        if !self.meta.hint_not_match(bucket_c, hash) {
            update.total_probes += 1;
            if self.buckets[bucket_c] == Some(key) {
                self.clear_bucket(bucket_c);
                update.total_writes += 1;
                return update;
            }
        }

        update
    }
}
