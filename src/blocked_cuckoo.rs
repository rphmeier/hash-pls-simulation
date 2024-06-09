use crate::{Map, Probe, Update};
use ahash::RandomState;
use rand::Rng;

const HASHER_COUNT: usize = 5;
const BUCKET_PER_BLOCK: usize = 107;

// dummy hash-set for u64 keys.
//
// implements blocked cuckoo hashing without any metamaps.
pub struct BlockedCuckoo {
    hashers: Vec<RandomState>,
    blocks: Vec<[Option<u64>; BUCKET_PER_BLOCK]>,
    len: usize,
}

impl BlockedCuckoo {
    pub fn new(capacity: usize) -> Self {
        let n_blocks = capacity / BUCKET_PER_BLOCK;
        BlockedCuckoo {
            hashers: (0..HASHER_COUNT).map(|_| RandomState::new()).collect(),
            blocks: vec![[None; BUCKET_PER_BLOCK]; n_blocks],
            len: 0,
        }
    }

    fn blocks(&self, key: u64) -> [usize; 2] {
        let hash_a = self.hashers[0].hash_one(key);
        let block_a = (hash_a % self.blocks.len() as u64) as usize;
        let mut block_b = block_a;

        let mut cur_hasher = 0;

        while block_b == block_a {
            cur_hasher += 1;

            block_b = (self.hashers[cur_hasher].hash_one(key) % self.blocks.len() as u64) as usize
        }

        [block_a, block_b]
    }

    fn try_insert(&mut self, block: usize, key: u64) -> Result<(), ()> {
        // maybe linear search is not the only way
        for val in self.blocks[block].iter_mut() {
            match val {
                None => {
                    *val = Some(key);
                    return Ok(());
                }
                Some(k) if *k == key => return Ok(()),
                _ => (),
            };
        }
        Err(())
    }

    fn try_remove(&mut self, block: usize, key: u64) -> Option<()> {
        if let Some(pos) = self.search(block, key) {
            self.blocks[block][pos] = None;
            return Some(());
        }
        None
    }

    // returns the index of the element
    // None if there is no such element
    fn search(&self, block: usize, key: u64) -> Option<usize> {
        for (index, val) in self.blocks[block].iter().enumerate() {
            match val {
                Some(k) if *k == key => {
                    return Some(index);
                }
                _ => (),
            };
        }
        None
    }
}

impl Map for BlockedCuckoo {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.blocks.len() * BUCKET_PER_BLOCK
    }

    fn probe(&self, key: u64) -> Probe {
        let [block_a, block_b] = self.blocks(key);
        let mut probe = Probe {
            contained: true,
            probes: 1,
        };

        if let Some(_pos) = self.search(block_a, key) {
            return probe;
        }

        probe.probes += 1;
        if let Some(_pos) = self.search(block_b, key) {
            return probe;
        }

        probe.contained = false;
        probe
    }

    fn insert(&mut self, key: u64) -> Update {
        const MAX_CHAIN: usize = 128;

        let mut rng = rand::thread_rng();

        let mut update = Update {
            total_probes: 0,
            total_writes: 1,
            completed: true,
        };

        self.len += 1;

        let [block_a, block_b] = self.blocks(key);

        update.total_probes += 1;
        if self.try_insert(block_a, key).is_ok() {
            return update;
        }

        update.total_probes += 1;
        if self.try_insert(block_b, key).is_ok() {
            return update;
        }

        // eviction process starts
        let mut active_key = key;
        // The target block could be random probably here
        let mut target_block = block_a;
        for _ in 0..MAX_CHAIN {
            let random_index = rng.gen_range(0..BUCKET_PER_BLOCK);
            let swap_key =
                self.blocks[target_block][random_index].expect("insert fail, there must be some");
            self.blocks[target_block][random_index] = Some(active_key);

            let [block_a, block_b] = self.blocks(swap_key);
            target_block = if block_a == target_block {
                block_b
            } else {
                block_a
            };

            update.total_writes += 1;
            update.total_probes += 1;
            if self.try_insert(target_block, swap_key).is_ok() {
                return update;
            }

            active_key = swap_key;
        }

        update.completed = false;
        update
    }

    fn remove(&mut self, key: u64) -> Update {
        let [block_a, block_b] = self.blocks(key);

        let mut update = Update {
            total_probes: 1,
            total_writes: 0,
            completed: true,
        };

        if self.try_remove(block_a, key).is_some() {
            self.len -= 1;
            update.total_writes += 1;
            return update;
        };

        update.total_probes += 1;
        if self.try_remove(block_b, key).is_some() {
            self.len -= 1;
            update.total_writes += 1;
            return update;
        };

        update.completed = false;
        update
    }
}
