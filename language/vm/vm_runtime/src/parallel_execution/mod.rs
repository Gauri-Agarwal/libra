use types::transaction::SignatureCheckedTransaction;

use std::collections::vec_deque::VecDeque;

pub struct DAGBoundaries {
    boundaries: Vec<VecDeque<(usize, SignatureCheckedTransaction)>>,
}

impl DAGBoundaries {
    pub fn new() -> Self {
        let boundaries = vec![VecDeque::new()];
        Self { boundaries }
    }

    pub fn insert(&mut self, level: usize, entry: (usize, SignatureCheckedTransaction)) {
        if level >= self.boundaries.len() {
            self.boundaries.push(VecDeque::new())
        }

        self.boundaries[level].push_back(entry);
    }

    pub fn num_levels(&self) -> usize {
        self.boundaries.len()
    }

    pub fn get_level(
        &mut self,
        level: usize,
    ) -> &mut VecDeque<(usize, SignatureCheckedTransaction)> {
        assert!(level < self.num_levels());
        &mut self.boundaries[level]
    }
}
