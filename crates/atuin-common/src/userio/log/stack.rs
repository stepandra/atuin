//! A stack of sinks, where the top-most [`userio::log::sink::Sink`] is the one that receives
//! messages.

use std::sync::Arc;

use parking_lot::Mutex;

use super::{Message, Sink, Stderr};

/// The stack the process's messages go to.
pub static SINK: SinkStack = SinkStack::new();

/// Sinks, the last one pushed and not yet dropped being current; [`Stderr`] when there are none.
#[derive(Default)]
pub struct SinkStack {
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    next_id: u64,
    sinks: Vec<(u64, Arc<dyn Sink>)>,
}

impl SinkStack {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Mutex::new(Entries {
                next_id: 0,
                sinks: Vec::new(),
            }),
        }
    }

    /// Make `sink` the current one until the guard drops.
    pub fn push(&self, sink: impl Sink + 'static) -> SinkGuard<'_> {
        let sink = Arc::new(sink);

        let mut entries = self.entries.lock();
        let id = entries.next_id;
        entries.next_id += 1;
        entries.sinks.push((id, sink));
        drop(entries);

        SinkGuard { stack: self, id }
    }

    /// Show `message` on the current sink.
    pub fn show(&self, message: Message) {
        // Called outside the lock, so a sink stalled on a write never holds up a push or a drop.
        let current = self.entries.lock().sinks.last().map(|(_, sink)| Arc::clone(sink));
        match current {
            Some(sink) => sink.show(message),
            None => Stderr::default().show(message),
        }
    }
}

/// Keeps a pushed sink on its stack; dropping it removes the sink wherever it is, in any order.
#[must_use = "the sink is removed as soon as the guard drops"]
pub struct SinkGuard<'a> {
    stack: &'a SinkStack,
    id: u64,
}

impl Drop for SinkGuard<'_> {
    fn drop(&mut self) {
        self.stack.entries.lock().sinks.retain(|(id, _)| *id != self.id);
    }
}
