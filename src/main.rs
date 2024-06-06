use csv::Writer;
use hdrhistogram::Histogram;
use rand::prelude::*;
use std::fs::File;

use cuckoo::Cuckoo;
use robinhood::RobinHood;
use three_ary_cuckoo::ThreeAryCuckoo;
use triangular_probing::TriaProb;

mod cuckoo;
mod meta_map;
mod robinhood;
mod three_ary_cuckoo;
mod triangular_probing;

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

struct Probe {
    // whether the key was contained.
    contained: bool,
    // number of probes _of buckets_, not metadata, needed.
    probes: usize,
}

// record of an update procedure.
struct Update {
    // the number of probes _of buckets_ made, in total.
    total_probes: usize,
    // the number of writes to buckets made, in total.
    // the number of keys which were moved by "robin hood" is equal to this minus 1.
    total_writes: usize,
    // Whether the update completed.
    completed: bool,
}

trait Map {
    fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity() as f64
    }
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;

    fn probe(&self, key: u64) -> Probe;
    fn insert(&mut self, key: u64) -> Update;
    fn remove(&mut self, key: u64) -> Update;
}

fn grow(map: &mut dyn Map, keys: &mut KeySet, increment: f64) -> Option<Record> {
    let mut probes = Histogram::new(3).unwrap();
    let mut writes = Histogram::new_with_bounds(1, u64::max_value(), 3).unwrap();

    let initial_load = map.load_factor();
    let load_target = initial_load + increment;
    while map.load_factor() < load_target {
        if map.len() == map.capacity() {
            break;
        }
        let update = map.insert(keys.push());

        if !update.completed {
            return None;
        } else if update.total_probes > 128 {
            return None;
        }

        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();
    }

    Some(Record {
        load_factor: initial_load,
        histograms: vec![probes, writes],
    })
}

fn probe(map: &dyn Map, keys: &KeySet, count: usize) -> Record {
    let mut present = Histogram::new(3).unwrap();
    let mut absent = Histogram::new(3).unwrap();

    let load_factor = map.load_factor();
    for _ in 0..count {
        let probe = map.probe(keys.existing());
        present.record(probe.probes as u64).unwrap();
        assert!(probe.contained);
    }
    for _ in 0..count {
        let probe = map.probe(keys.nonexisting());
        absent.record(probe.probes as u64).unwrap();
        assert!(!probe.contained);
    }

    Record {
        load_factor,
        histograms: vec![present, absent],
    }
}

fn churn(map: &mut dyn Map, keys: &mut KeySet, count: usize) -> Record {
    let mut probes = Histogram::new(3).unwrap();
    let mut writes = Histogram::new_with_bounds(1, u64::max_value(), 3).unwrap();

    let load_factor = map.load_factor();
    for _ in 0..count {
        let update = map.remove(keys.pop());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();

        let update = map.insert(keys.push());
        probes.record(update.total_probes as u64).unwrap();
        writes.record(update.total_writes as u64).unwrap();
    }

    Record {
        load_factor,
        histograms: vec![probes, writes],
    }
}

struct Record {
    load_factor: f64,
    histograms: Vec<Histogram<u64>>,
}

impl Record {
    fn write(&self, writer: &mut Writer<File>, map_spec: MapSpec) {
        let mut csv_data = vec![
            format!("{:.2}", self.load_factor),
            format!("{}", map_spec.size()),
            format!("{}", map_spec.meta_bits()),
        ];
        let histogram_data = self.histograms.iter().flat_map(|h| {
            vec![
                h.mean(),
                h.value_at_percentile(50.0) as f64,
                h.value_at_percentile(95.0) as f64,
                h.value_at_percentile(99.0) as f64,
            ]
            .into_iter()
            .map(|value| format!("{value:.2}"))
        });

        csv_data.extend(histogram_data);

        writer.write_record(csv_data).unwrap();

        writer.flush().unwrap();
    }
}

struct Writers {
    grow: Writer<File>,
    probe: Writer<File>,
    churn: Writer<File>,
}

impl Writers {
    fn build(name: String) -> Self {
        Writers {
            grow: Writer::from_path(format!("out/grow_{name}.csv")).unwrap(),
            probe: Writer::from_path(format!("out/probe_{name}.csv")).unwrap(),
            churn: Writer::from_path(format!("out/churn_{name}.csv")).unwrap(),
        }
    }
}

const SIZE: usize = 1 << 20;

#[derive(Clone, Copy)]
enum MapSpec {
    RobinHood(usize),
    Cuckoo(usize),
    ThreeAryCuckoo(usize),
    TriaProb(usize),
}

impl MapSpec {
    fn build(&self) -> Box<dyn Map> {
        match *self {
            MapSpec::RobinHood(meta_bits) => Box::new(RobinHood::new(SIZE, meta_bits)),
            MapSpec::Cuckoo(meta_bits) => Box::new(Cuckoo::new(SIZE, meta_bits)),
            MapSpec::ThreeAryCuckoo(meta_bits) => Box::new(ThreeAryCuckoo::new(SIZE, meta_bits)),
            MapSpec::TriaProb(meta_bits) => Box::new(TriaProb::new(SIZE, meta_bits)),
        }
    }

    fn size(&self) -> usize {
        SIZE
    }

    fn meta_bits(&self) -> usize {
        match *self {
            MapSpec::RobinHood(meta_bits) => meta_bits,
            MapSpec::Cuckoo(meta_bits) => meta_bits,
            MapSpec::ThreeAryCuckoo(meta_bits) => meta_bits,
            MapSpec::TriaProb(meta_bits) => meta_bits,
        }
    }
}

fn grow_test(writers: &mut Writers, map_spec: MapSpec) {
    const INCREMENT: f64 = 0.01;
    const MAX_LOAD: f64 = 0.98;

    let mut map = map_spec.build();
    let mut key_set = KeySet::default();
    while map.load_factor() + INCREMENT < MAX_LOAD {
        if let Some(record) = grow(&mut *map, &mut key_set, INCREMENT) {
            record.write(&mut writers.grow, map_spec);
        } else {
            break;
        }
    }
}

fn probe_test(writers: &mut Writers, map_spec: MapSpec) {
    const INCREMENT: f64 = 0.02;
    const MAX_LOAD: f64 = 0.98;

    let mut load = 0.1;
    while load <= MAX_LOAD {
        let mut map = map_spec.build();
        let mut key_set = KeySet::default();
        if grow(&mut *map, &mut key_set, load).is_none() {
            break;
        };

        let record = probe(&*map, &key_set, 10_000);
        record.write(&mut writers.probe, map_spec);
        load += INCREMENT;
    }
}

fn churn_test(writers: &mut Writers, map_spec: MapSpec) {
    const INCREMENT: f64 = 0.02;
    const MAX_LOAD: f64 = 0.98;

    let mut load = 0.1;
    while load <= MAX_LOAD {
        let mut map = map_spec.build();
        let mut key_set = KeySet::default();
        if grow(&mut *map, &mut key_set, load).is_none() {
            break;
        };

        let record = churn(&mut *map, &mut key_set, 10_000);
        record.write(&mut writers.churn, map_spec);
        load += INCREMENT;
    }
}

fn main() {
    std::fs::create_dir_all("out").unwrap();

    let mut writers = Writers::build(format!("robinhood"));
    for meta_bits in [0, 1, 2, 4, 8] {
        println!("robinhood {meta_bits}");
        let map_spec = MapSpec::RobinHood(meta_bits);
        grow_test(&mut writers, map_spec);
        probe_test(&mut writers, map_spec);
        churn_test(&mut writers, map_spec);
    }

    let mut writers = Writers::build(format!("cuckoo"));
    for meta_bits in [0, 1, 2, 4, 8] {
        println!("cuckoo {meta_bits}");

        let map_spec = MapSpec::Cuckoo(meta_bits);
        grow_test(&mut writers, map_spec);
        probe_test(&mut writers, map_spec);
        churn_test(&mut writers, map_spec);
    }

    let mut writers = Writers::build(format!("3arycuckoo"));
    for meta_bits in [0, 1, 2, 4, 8] {
        println!("3arycuckoo {meta_bits}");

        let map_spec = MapSpec::ThreeAryCuckoo(meta_bits);
        grow_test(&mut writers, map_spec);
        probe_test(&mut writers, map_spec);
        churn_test(&mut writers, map_spec);
    }

    let mut writers = Writers::build(format!("triaprob"));
    for meta_bits in [0, 1, 2, 4, 8] {
        println!("triangular_probing {meta_bits}");

        let map_spec = MapSpec::TriaProb(meta_bits);
        grow_test(&mut writers, map_spec);
        probe_test(&mut writers, map_spec);
        churn_test(&mut writers, map_spec);
    }
}
