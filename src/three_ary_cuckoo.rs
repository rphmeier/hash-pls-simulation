use crate::meta_map::{MetaMap, Metadata};
use crate::{Map, Probe, Update};
use ahash::RandomState;

const HASHER_COUNT: usize = 3;

// dummy hash-set for u64 keys.
//
// implements 3-ary cuckoo hashing.
pub struct ThreeAryCuckoo {
    hashers: Vec<RandomState>,
    bucket_size: usize,
    buckets: Vec<Option<u64>>,
    meta: MetaMap,
    len: usize,
}

impl ThreeAryCuckoo {
    pub fn new(capacity: usize, meta_bits: usize) -> Self {
        ThreeAryCuckoo {
            hashers: (0..HASHER_COUNT).map(|_| RandomState::new()).collect(),
            buckets: vec![None; capacity],
            bucket_size: capacity / 3,
            meta: MetaMap::new(capacity, meta_bits),
            len: 0,
        }
    }

    // (hash, [bucket_a, bucket_b, bucket_c])
    fn buckets(&self, key: u64) -> (u64, [usize; 3]) {
        let hash_a = self.hashers[0].hash_one(key);
        let bucket_a = (hash_a % self.bucket_size as u64) as usize;
        let mut bucket_b = (self.hashers[1].hash_one(key) % self.bucket_size as u64) as usize;
        let mut bucket_c = (self.hashers[2].hash_one(key) % self.bucket_size as u64) as usize;

        // each bucket has the same size, `self.bucket_size`,
        // but they live in the same array just with different offsets
        bucket_b += self.bucket_size;
        bucket_c += 2 * self.bucket_size;

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

            // TODO: not sure why in cuckoo.rs the presence check
            // is applied only to bucket_b and not bucket_a
            //
            // I think that this early check could be applied to all buckets
            if !self.meta.hint_not_match(bucket_a, hash) {
                update.total_probes += 1;
                if self.buckets[bucket_a] == Some(key) {
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

        let mut target_bucket_index = 0;
        for _ in 0..MAX_CHAIN {
            let (hash, buckets) = key_info;
            let target_bucket = buckets[target_bucket_index];

            if self.meta.hint_empty(target_bucket) {
                if active_key != key {
                    update.total_writes += 1;
                }
                self.set_bucket(target_bucket, active_key, hash);
                return update;
            }

            update.total_probes += 1;
            let swap_key = match self.buckets[target_bucket] {
                None => {
                    // This is reach only if metabits = 0
                    // thus there is not knowledge of presence
                    if active_key != key {
                        update.total_writes += 1;
                    }

                    self.set_bucket(target_bucket, active_key, hash);
                    return update;
                }
                Some(k) => {
                    if k == active_key {
                        // TODO: In which scenario is this reachable?
                        unreachable!();
                    }

                    update.total_writes += 1;
                    self.set_bucket(target_bucket, active_key, hash);
                    k
                }
            };

            key_info = self.buckets(swap_key);

            // take the next bucket
            target_bucket_index = (target_bucket_index + 1) % 3;
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
