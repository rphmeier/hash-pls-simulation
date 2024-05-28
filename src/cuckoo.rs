use crate::meta_map::{MetaMap, Metadata};
use crate::{Map, Probe, Update};
use ahash::RandomState;

const HASHER_COUNT: usize = 5;

// dummy hash-set for u64 keys.
//
// implements cuckoo hashing.
pub struct Cuckoo {
    hashers: Vec<RandomState>,
    buckets: Vec<Option<u64>>,
    meta: MetaMap,
    len: usize,
}

impl Cuckoo {
    pub fn new(capacity: usize, meta_bits: usize) -> Self {
        Cuckoo {
            hashers: (0..HASHER_COUNT).map(|_| RandomState::new()).collect(),
            buckets: vec![None; capacity],
            meta: MetaMap::new(capacity, meta_bits),
            len: 0,
        }
    }

    fn buckets(&self, key: u64) -> (u64, usize, usize) {
        let hash_a = self.hashers[0].hash_one(key);
        let bucket_a = (hash_a % self.buckets.len() as u64) as usize;
        let mut bucket_b = bucket_a;

        let mut cur_hasher = 0;

        while bucket_b == bucket_a {
            cur_hasher += 1;

            bucket_b = (self.hashers[cur_hasher].hash_one(key) % self.buckets.len() as u64) as usize
        }

        (hash_a, bucket_a, bucket_b)
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

impl Map for Cuckoo {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn probe(&self, key: u64) -> Probe {
        let (hash, bucket_a, bucket_b) = self.buckets(key);

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
        let mut use_bucket_a = true;
        let mut key_info = self.buckets(key);

        // test for presence.
        {
            let (hash, _, bucket_b) = key_info;
            if !self.meta.hint_not_match(bucket_b, hash) {
                update.total_probes += 1;
                if self.buckets[bucket_b] == Some(key) {
                    return update;
                }
            }
        }

        self.len += 1;

        for _ in 0..MAX_CHAIN {
            let (hash, bucket_a, bucket_b) = key_info;
            let target_bucket = if use_bucket_a { bucket_a } else { bucket_b };

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
                    if active_key != key {
                        update.total_writes += 1;
                    }

                    self.set_bucket(target_bucket, active_key, hash);
                    return update;
                }
                Some(k) => {
                    if k == active_key {
                        assert_eq!(active_key, key);
                        // this can only happen for the initial key because bucket_a != bucket_b
                        self.len -= 1;
                        return update;
                    }

                    update.total_writes += 1;
                    self.set_bucket(target_bucket, active_key, hash);
                    k
                }
            };

            key_info = self.buckets(swap_key);

            // if this is our next key's "bucket b", use it's bucket a.
            use_bucket_a = key_info.2 == target_bucket;
            active_key = swap_key;
        }

        update.completed = false;
        update
    }

    fn remove(&mut self, key: u64) -> Update {
        let (hash, bucket_a, bucket_b) = self.buckets(key);

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

        update
    }
}
