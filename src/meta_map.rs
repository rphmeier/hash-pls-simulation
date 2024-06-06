use bitvec::prelude::*;

pub struct MetaMap {
    bits: usize,
    bitvec: BitVec<u64, Msb0>,
}

pub enum Metadata {
    Hash(u64),
    Psl(usize),
}

pub enum PslHint {
    Exact(usize),
    AtLeast(usize),
}

// EMPTY: `0 | 0s`
// TOMBSTONE: `0 | 1s` except if bits == 1 - then `1`.
// FULL: `1 | metadata bits`

impl MetaMap {
    pub fn new(buckets: usize, bits_per_bucket: usize) -> Self {
        MetaMap {
            bits: bits_per_bucket,
            bitvec: bitvec![u64, Msb0; 0; buckets * bits_per_bucket],
        }
    }

    pub fn set_full(&mut self, bucket: usize, metadata: Metadata) {
        if self.bits == 0 {
            return;
        }
        if self.bits == 1 {
            self.bitvec.set(bucket, true);
            return;
        }

        let bits_remaining = self.bits - 1;
        let start = bucket * self.bits;
        let end = start + self.bits;

        self.bitvec.set(start, true);

        match metadata {
            Metadata::Hash(raw_hash) => {
                let high_bits = &raw_hash.view_bits::<Msb0>()[..bits_remaining];
                self.bitvec[start + 1..end].copy_from_bitslice(high_bits);
            }
            Metadata::Psl(psl) => {
                let truncated = std::cmp::min(psl, 1 << bits_remaining) - 1;
                let psl_bits = truncated.view_bits::<Msb0>();
                let psl_start = psl_bits.len() - bits_remaining;
                self.bitvec[start + 1..end].clone_from_bitslice(&psl_bits[psl_start..]);
            }
        }
    }

    pub fn set_empty(&mut self, bucket: usize) {
        if self.bits == 0 {
            return;
        }
        if self.bits == 1 {
            self.bitvec.set(bucket, false);
            return;
        }

        self.bitvec.set(bucket * self.bits, false);
        for i in 1..self.bits {
            self.bitvec.set(bucket * self.bits + i, false);
        }
    }

    pub fn set_tombstone(&mut self, bucket: usize) {
        if self.bits == 0 {
            return;
        }
        if self.bits == 1 {
            self.bitvec.set(bucket, true);
            return;
        }

        self.bitvec.set(bucket * self.bits, false);
        for i in 1..self.bits {
            self.bitvec.set(bucket * self.bits + i, true);
        }
    }

    // Get the number of bits in the meta-map.
    pub fn bits(&self) -> usize {
        self.bits
    }

    // true means definitely empty.
    pub fn hint_empty(&self, bucket: usize) -> bool {
        if self.bits == 0 {
            return false;
        }
        if self.bits == 1 {
            return !self.bitvec.get(bucket).unwrap();
        }

        let start = bucket * self.bits;
        let end = start + self.bits;
        self.bitvec[start..end].not_any()
    }

    // true means definitely a tombstone.
    pub fn hint_tombstone(&self, bucket: usize) -> bool {
        if self.bits <= 1 {
            return false;
        }

        let start = bucket * self.bits;
        let end = start + self.bits;

        !self.bitvec.get(start).unwrap() && self.bitvec[start + 1..end].all()
    }

    pub fn hint_psl(&self, bucket: usize) -> Option<PslHint> {
        if self.bits == 0 {
            return None;
        }
        if self.bits == 1 {
            return if *self.bitvec.get(bucket).unwrap() {
                Some(PslHint::AtLeast(1))
            } else {
                None
            };
        }

        let start = bucket * self.bits;
        let end = start + self.bits;
        if *self.bitvec.get(start).unwrap() {
            let psl_bits = &self.bitvec[start + 1..end];
            if psl_bits.all() {
                Some(PslHint::AtLeast(1 << self.bits - 1))
            } else {
                Some(PslHint::Exact(psl_bits.load::<usize>() + 1))
            }
        } else {
            None
        }
    }

    // returns true if it's definitely not a match.
    pub fn hint_not_match(&self, bucket: usize, raw_hash: u64) -> bool {
        if self.bits == 0 {
            return false;
        }
        if self.bits == 1 {
            return !*self.bitvec.get(bucket).unwrap();
        }

        let bits_remaining = self.bits - 1;
        let start = bucket * self.bits;
        let end = start + self.bits;

        !*self.bitvec.get(start).unwrap() || {
            let high_bits = &raw_hash.view_bits::<Msb0>()[..bits_remaining];
            &self.bitvec[start + 1..end] != high_bits
        }
    }
}
