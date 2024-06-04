use std::collections::HashSet;

use crate::meta_map::{MetaMap, Metadata};
use crate::{Map, Probe, Update};
use ahash::RandomState;

#[derive(Clone)]
enum BucketItem {
    Value(u64),
    // While searching for a key only founding the key itself
    // or and empty bucket could stop the search
    Empty,
    // When inserting tombsone can be used
    Tombstone,
}

// dummy hash-set for u64 keys.
//
// implements tringular probing on top of a plain array.
//
// MetaMap keeps the same state of what's in memery but just
// using some bits
pub struct TriaProb {
    hasher: RandomState,
    buckets: Vec<BucketItem>,
    //meta: MetaMap,
    len: usize,
}

impl TriaProb {
    pub fn new(capacity: usize, _meta_bits: usize) -> Self {
        TriaProb {
            hasher: RandomState::new(),
            buckets: vec![BucketItem::Empty; capacity],
            //meta: MetaMap::new(capacity, meta_bits),
            len: 0,
        }
    }

    // Given a key apply triangular probing starting from the
    // bucket associated with the key and contiue until the key
    // is found or if empty specified to true, stop as soon as one empty
    // slot is found
    //
    // return a tuple containgins:
    //  + option containing the index of the slot
    //  + number of probes
    fn probe_search(&self, key: u64) -> (Option<usize>, usize) {
        let bucket = (self.hasher.hash_one(key) % (self.buckets.len() as u64)) as usize;
        let mut offset = 0;

        // All bucket needs to be iterated
        for i in 0..self.buckets.len() {
            offset += i;
            let bucket_index = (bucket + offset) % self.buckets.len();
            match self.buckets[bucket_index] {
                BucketItem::Value(found_key) if key == found_key => return (Some(bucket_index), i),
                // If an empty is found, return independently
                BucketItem::Empty => return (None, i),
                _ => (),
            };
        }

        (None, self.buckets.len())
    }

    fn probe_insert(&self, key: u64) -> (Option<usize>, usize) {
        let bucket = (self.hasher.hash_one(key) % (self.buckets.len() as u64)) as usize;
        let mut offset = 0;

        let mut visited_bucket_set = HashSet::new();

        // All bucket needs to be iterated
        for i in 0..self.buckets.len() {
            offset += i;
            let bucket_index = (bucket + offset) % self.buckets.len();

            assert!(visited_bucket_set.insert(bucket_index));

            //if i >= 3 {
            //    println!("key: {key}, bucket: {bucket}, bucket_index: {bucket_index}");
            //}
            match self.buckets[bucket_index] {
                // If an empty is found, return independently
                //
                // Tombstone are a good result for seach only if the probe is not
                // meant to search a value, but maybe to insert a new one
                BucketItem::Empty | BucketItem::Tombstone => return (Some(bucket_index), i),
                BucketItem::Value(found_key) if key == found_key => return (Some(bucket_index), i),
                _ => (),
            };
        }

        for i in 0..self.buckets.len() {
            assert!(visited_bucket_set.contains(&i));
        }

        println!(
            "Not found place for insert, key: {}, bucket: {}",
            key, bucket
        );
        (None, self.buckets.len())
    }

    fn set_bucket(&mut self, bucket: usize, item: BucketItem /*, hash: u64*/) {
        self.buckets[bucket] = item;
        //self.meta.set_full(bucket, Metadata::Hash(hash));
    }

    // Until something like compaction is implementd this will never be used
    //fn empty_bucket(&mut self, bucket: usize) {
    //    self.buckets[bucket] = BucketItem::Empty;
    //    //self.meta.set_empty(bucket);
    //}
}

impl Map for TriaProb {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn probe(&self, key: u64) -> Probe {
        // TODO: meta map search
        //if !self.meta.hint_not_match(bucket_a, hash) {
        //    probes += 1;
        //    if self.buckets[bucket_a] == Some(key) {
        //        return Probe {
        //            contained: true,
        //            probes,
        //        };
        //    }
        //}

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

        // TODO: use metamap to check for presence
        // test for presence.
        //{
        //    let (hash, _, bucket_b) = key_info;
        //    if !self.meta.hint_not_match(bucket_b, hash) {
        //        update.total_probes += 1;
        //        if self.buckets[bucket_b] == Some(key) {
        //            return update;
        //        }
        //    }
        //}

        let (probe_result, total_probes) = self.probe_insert(key);
        update.total_probes = total_probes;

        let Some(bucket_index) = probe_result else {
            println!("total_probes, {total_probes}");
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

        // TODO: use metamap to check for presence
        // test for presence.
        //{
        //    let (hash, _, bucket_b) = key_info;
        //    if !self.meta.hint_not_match(bucket_b, hash) {
        //        update.total_probes += 1;
        //        if self.buckets[bucket_b] == Some(key) {
        //            return update;
        //        }
        //    }
        //}

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
