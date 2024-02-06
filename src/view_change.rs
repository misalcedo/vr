use std::cmp::Ordering;
use std::collections::HashMap;
use crate::model::DoViewChange;
use crate::stamps::View;

#[derive(Debug, Default)]
pub struct ViewChangeBuffer {
    view: View,
    buffer: HashMap<usize, DoViewChange>
}

impl ViewChangeBuffer {
    pub fn insert(&mut self, message: DoViewChange) {
        match message.v.cmp(&self.view) {
            Ordering::Less => (),
            Ordering::Equal => {
                self.buffer.insert(message.i, message);
            }
            Ordering::Greater => {
                self.view = message.v;
                self.buffer.clear();
                self.buffer.insert(message.i, message);
            }
        }
    }

    pub fn start_view(&mut self, index: usize, group_size: usize) -> Option<DoViewChange> {
        if self.is_complete(index, group_size) {
            let message = self.buffer.drain()
                .map(|(_, v)| v)
                .max_by_key(|do_view_change| do_view_change.t.last_entry())?;

            Some(message)
        } else {
            None
        }
    }

    fn is_complete(&self, index: usize, group_size: usize) -> bool {
        let majority = (group_size / 2) + 1;

        self.buffer.contains_key(&index) && self.buffer.len() >= majority
    }
}