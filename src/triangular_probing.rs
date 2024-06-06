use std::collections::HashSet;

use crate::meta_map::{MetaMap, Metadata};
use crate::{Map, Probe, Update};
use ahash::RandomState;

#[derive(Clone, Debug)]
enum BucketItem {
    // While searching for a key, only finding the key itself
    // or an empty bucket could stop the search
    Value(u64),
    Empty,
    // When a new value needs to be inserted,
    // tombsones can be used
    Tombstone,
}

// dummy hash-set for u64 keys.
//
// implements tringular probing.
pub struct TriaProb {
    hasher: RandomState,
    buckets: Vec<BucketItem>,
    meta: MetaMap,
    len: usize,
}

impl TriaProb {
    pub fn new(capacity: usize, meta_bits: usize) -> Self {
        TriaProb {
            hasher: RandomState::new(),
            buckets: vec![BucketItem::Empty; capacity],
            meta: MetaMap::new(capacity, meta_bits),
            len: 0,
        }
    }

    // To search a key triangular probing is applied,
    // it ends only if the searched key is found or en empty bucket is found
    //
    // return a tuple containgins:
    //  + option containing the index of the slot
    //  + number of probes
    fn probe_search(&self, key: u64) -> (Option<usize>, usize) {
        let hash = self.hasher.hash_one(key);
        let bucket = (hash % (self.buckets.len() as u64)) as usize;
        let mut offset = 0;
        let mut probes = 0;

        // All bucket could be iterated
        for i in 0..self.buckets.len() {
            offset += i;
            let bucket_index = (bucket + offset) % self.buckets.len();

            // The probing will be performed on the metamap and only if needed the check will be propagated

            if self.meta.hint_empty(bucket_index) {
                // If an empty is found in the metamap we're sure it is empty also in the buckets
                return (None, probes);
            }

            // We will check the value stored in storage only when there is a match in the metamap
            if !self.meta.hint_not_match(bucket_index, hash) {
                probes += 1;
                match &self.buckets[bucket_index] {
                    // we want to be sure this is the correct bucket_index
                    BucketItem::Value(found_key) if key == *found_key => {
                        return (Some(bucket_index), probes)
                    }
                    BucketItem::Empty => {
                        // This should be reached ONLY if the map uses 0 metabits
                        assert!(self.meta.bits() == 0);
                        return (None, probes);
                    }
                    BucketItem::Tombstone => {
                        // This should be reached ONLY if the map uses less than 2 metabits
                        assert!(self.meta.bits() < 2);
                    }
                    _ => (),
                };
            };

            // If it does not match in the metamap, then we can proceed with the probing
        }

        (None, self.buckets.len())
    }

    // In order to insert a value into the hash map,
    // we need to search for the key we intend to insert,
    // an empty bucket, or a tombstone.
    fn probe_insert(&self, key: u64) -> (Option<usize>, usize) {
        let hash = self.hasher.hash_one(key);
        let bucket = (hash % (self.buckets.len() as u64)) as usize;
        let mut offset = 0;
        let mut probes = 0;

        // All bucket could be iterated
        for i in 0..self.buckets.len() {
            offset += i;
            let bucket_index = (bucket + offset) % self.buckets.len();

            if self.meta.hint_empty(bucket_index) || self.meta.hint_tombstone(bucket_index) {
                return (Some(bucket_index), probes);
            }

            if !self.meta.hint_not_match(bucket_index, hash) {
                probes += 1;
                match self.buckets[bucket_index] {
                    BucketItem::Empty => {
                        assert_eq!(self.meta.bits(), 0);
                        return (Some(bucket_index), probes);
                    }
                    BucketItem::Tombstone => {
                        assert!(self.meta.bits() < 2);
                        return (Some(bucket_index), probes);
                    }
                    BucketItem::Value(found_key) if key == found_key => {
                        return (Some(bucket_index), probes)
                    }
                    _ => (),
                };
            }
        }

        (None, self.buckets.len())
    }

    fn set_bucket(&mut self, bucket: usize, item: BucketItem) {
        match item {
            BucketItem::Value(key) => {
                let hash = self.hasher.hash_one(key);
                self.meta.set_full(bucket, Metadata::Hash(hash));
            }
            // this will not be used until something like compaction is implemented
            BucketItem::Empty => unreachable!(),
            BucketItem::Tombstone => {
                self.meta.set_tombstone(bucket);
            }
        }
        self.buckets[bucket] = item;
    }
}

impl Map for TriaProb {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn probe(&self, key: u64) -> Probe {
        let (probe_result, probes) = self.probe_search(key);

        Probe {
            contained: probe_result.is_some(),
            probes,
        }
    }

    fn insert(&mut self, key: u64) -> Update {
        let mut update = Update {
            total_probes: 0,
            total_writes: 1,
            completed: true,
        };

        let (probe_result, total_probes) = self.probe_insert(key);
        update.total_probes = total_probes;

        let Some(bucket_index) = probe_result else {
            update.completed = false;
            return update;
        };

        self.len += 1;
        self.set_bucket(bucket_index, BucketItem::Value(key));

        update
    }

    fn remove(&mut self, key: u64) -> Update {
        let mut update = Update {
            total_probes: 0,
            total_writes: 1,
            completed: true,
        };

        let (probe_result, total_probes) = self.probe_search(key);
        let Some(bucket_index) = probe_result else {
            update.completed = false;
            update.total_probes = total_probes;
            return update;
        };

        self.len -= 1;
        self.set_bucket(bucket_index, BucketItem::Tombstone);
        update
    }
}
