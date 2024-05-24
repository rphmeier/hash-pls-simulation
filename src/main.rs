use bitvec::prelude::*;
use ahash::{AHasher, RandomState};
use hdrhistogram::Histogram;
use std::hash::{BuildHasher, Hasher};
use rand::prelude::*;

struct Probe {
    // whether the key was contained.
    contained: bool,
    // number of probes needed.
    psl: usize,
}

// record of an update procedure.
struct Update {
    // the number of probes made, in total.
    total_probes: usize,
    // the number of writes made, in total.
    // the number of keys which were moved by "robin hood" is equal to this minus 1.
    total_writes: usize,
}

// dummy hash-set for u64 keys.
//
// implements robin-hood-hashing with backward-shift deletion
struct RobinHood {
    hasher: RandomState,
    buckets: Vec<Option<u64>>,
    len: usize,
}

impl RobinHood {
    fn new(capacity: usize) -> Self {
        RobinHood {
            hasher: RandomState::new(),
            buckets: vec![None; capacity],
            len: 0,
        }
    } 

    fn len(&self) -> usize {
        self.len
    }

    fn bucket_for(&self, key: u64) -> usize {
        (self.hasher.hash_one(key) % (self.buckets.len() as u64)) as usize
    }

    fn load_factor(&self) -> f64 {
        (self.len as f64) / (self.buckets.len() as f64)
    }

    fn probe(&self, key: u64) -> Probe {
        let mut psl = 1;
        let mut bucket = self.bucket_for(key);
        loop {
            match self.buckets[bucket] {
                None => return Probe { contained: false, psl },
                Some(k) if k == key => return Probe { contained: true, psl },
                Some(k) => {

                    if self.psl_of(k, bucket) < psl {
                        return Probe {
                            contained: false,
                            psl,
                        }
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
    fn insert(&mut self, key: u64) -> Update {
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
                return update
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
    fn remove(&mut self, key: u64) -> Update {
        let probe = self.probe(key);
        let mut update = Update {
            total_probes: probe.psl,
            total_writes: 0,
        };

        if !probe.contained {
            return update
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

#[derive(Default)]
struct KeySet {
    max: u64,
    min: u64,
}

impl KeySet {
    fn len(&self) -> usize {
        (self.max - self.min) as usize
    }

    fn push(&mut self) -> u64 {
        self.max += 1;
        self.max - 1
    }

    fn pop(&mut self) -> u64 {
        assert!(self.max > self.min);
        self.min += 1;
        self.min
    }

    fn existing(&self) -> u64 {
        thread_rng().gen_range(self.min + 1..self.max)
    }

    fn nonexisting(&self) -> u64 {
        thread_rng().gen_range(self.max..u64::MAX)
    }
}

fn populate(map: &mut RobinHood, keys: &mut KeySet, increment: f64) -> (Histogram<u64>, Histogram<u64>) {
    let mut probes = Histogram::new(3).unwrap();
    let mut writes = Histogram::new(3).unwrap();

    let load_target = map.load_factor() + increment;
    while map.load_factor() < load_target {
        if map.len == map.buckets.len() { break }
        let update = map.insert(keys.push());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();
    }

    (probes, writes)
}

fn probe_existing(map: &RobinHood, keys: &KeySet, count: usize) -> Histogram<u64> {
    let mut probes = Histogram::new(3).unwrap();

    for _ in 0..count {
        let probe = map.probe(keys.existing());

        probes.record(probe.psl as u64).unwrap();
    }

    probes
}

fn probe_non_existing(map: &RobinHood, keys: &KeySet, count: usize) -> Histogram<u64> {
    let mut probes = Histogram::new(3).unwrap();

    for _ in 0..count {
        let probe = map.probe(keys.nonexisting());

        probes.record(probe.psl as u64).unwrap();
    }

    probes
}

fn churn(map: &mut RobinHood, keys: &mut KeySet, count: usize) -> (Histogram<u64>, Histogram<u64>) {
    let mut probes = Histogram::new(3).unwrap();
    let mut writes = Histogram::new(3).unwrap();

    for _ in 0..count {
        let update = map.remove(keys.pop());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();

        let update = map.insert(keys.push());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();
    }

    (probes, writes)
}

fn overwrite_existing(map: &mut RobinHood, keys: &mut KeySet, count: usize) -> (Histogram<u64>, Histogram<u64>) {
    let mut probes = Histogram::new(3).unwrap();
    let mut writes = Histogram::new(3).unwrap();

    for _ in 0..count {
        let update = map.insert(keys.existing());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();
    }

    (probes, writes)
}

fn print_probe_data(probe_data: Histogram<u64>) {
    println!("MEAN\t| probes={}", probe_data.mean());
    println!("50th\t| probes={}", probe_data.value_at_percentile(50.0));
    println!("95th\t| probes={}", probe_data.value_at_percentile(95.0));
    println!("99th\t| probes={}", probe_data.value_at_percentile(99.0));
    println!("----------");
}

fn print_data(probe_data: Histogram<u64>, write_data: Histogram<u64>) {
    println!("MEAN\t| probes={} | writes={}", probe_data.mean(), write_data.mean());
    println!("50th\t| probes={} | writes={}", probe_data.value_at_percentile(50.0), write_data.value_at_percentile(50.0));
    println!("95th\t| probes={} | writes={}", probe_data.value_at_percentile(95.0), write_data.value_at_percentile(95.0));
    println!("99th\t| probes={} | writes={}", probe_data.value_at_percentile(99.0), write_data.value_at_percentile(99.0));
    println!("----------");
}

fn main() {
    for size in [1_000_000] {
        println!("TESTING {size}...");
        let mut map = RobinHood::new(size);

        let mut key_set = KeySet::default();
        for _ in 0..45 {
            let prev_load = map.load_factor();

            let (probe_data, write_data) = populate(&mut map, &mut key_set, 0.02);

            println!("----------");
            println!("|  {:.2}  |", map.load_factor());
            println!("----------");

            println!("INSERT from {:.2} to {:.2}:", prev_load, map.load_factor());
            print_data(probe_data, write_data);

            println!("PROBE EXISTING 10,000");
            let probe_data = probe_existing(&mut map, &mut key_set, 10_000);
            print_probe_data(probe_data);

            println!("PROBE NONEXISTING 10,000");
            let probe_data = probe_non_existing(&mut map, &mut key_set, 10_000);
            print_probe_data(probe_data);

            println!("CHURN 10,000");
            let (probe_data, write_data) = churn(&mut map, &mut key_set, 10_000);
            print_data(probe_data, write_data);

            println!("OVERWRITE 10,000");
            let (probe_data, write_data) = overwrite_existing(&mut map, &mut key_set, 10_000);
            print_data(probe_data, write_data);
        }
    }
}
