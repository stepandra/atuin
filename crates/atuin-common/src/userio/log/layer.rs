//! The [`tracing`] layer that shows [`USER`] events on a [`SinkStack`].

use std::fmt;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

use super::{Message, Severity, SinkStack, USER};

/// Shows every [`USER`] event on the current sink of a [`SinkStack`].
pub struct UserLayer {
    stack: &'static SinkStack,
}

impl UserLayer {
    #[must_use]
    pub const fn new(stack: &'static SinkStack) -> Self {
        Self { stack }
    }
}

impl<S: Subscriber> Layer<S> for UserLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let metadata = event.metadata();

        if metadata.target() != USER {
            return;
        }

        let Some(severity) = Severity::from_level(*metadata.level()) else {
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
