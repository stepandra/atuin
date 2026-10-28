//! The [`tracing`] layer that shows [`USER`] events on a [`SinkStack`].

use std::fmt;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::{Filtered, LevelFilter, Targets};
use tracing_subscriber::layer::{Context, Layer};

use super::{Message, Severity, SinkStack, USER};

/// Shows every [`USER`] event on the current sink of a [`SinkStack`].
///
/// Every subscriber needs one, and its other layers must leave [`USER`] events alone: a message
/// shows on the sink, not in logs or traces.
pub struct UserLayer {
    stack: &'static SinkStack,
}

impl UserLayer {
    /// The layer for `stack`, with a filter of its own: it takes every [`USER`] event whatever the
    /// subscriber's other filters drop, so the arguments of `info!`, `warn!` and `error!` always run,
    /// and no other event.
    ///
    /// Add it last: a sink may log, and an event logged while another is dispatched clobbers the
    /// per-layer filter state of the layers still to see the first one.
    #[must_use]
    pub fn new<S: Subscriber>(stack: &'static SinkStack) -> Filtered<Self, Targets, S> {
        Self { stack }.with_filter(Targets::new().with_target(USER, LevelFilter::INFO))
    }
}

impl<S: Subscriber> Layer<S> for UserLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let Some(severity) = Severity::from_level(*event.metadata().level()) else {
            return;
        };

        let mut text = Text(None);
        event.record(&mut text);
        let Some(text) = text.0 else {
            return;
        };

        self.stack.show(Message { severity, text });
    }
}

impl Severity {
    /// The severity of an event at `level`, if users see that level.
    const fn from_level(level: Level) -> Option<Self> {
        match level {
            Level::ERROR => Some(Self::Error),
            Level::WARN => Some(Self::Warn),
            Level::INFO => Some(Self::Info),
            _ => None,
        }
    }
}

/// An event's `message` field.
struct Text(Option<String>);

impl Visit for Text {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}
