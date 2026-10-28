//! Model conversion utilities for the `user_messages` gRPC protobuf.
mod codegen {
    #![allow(clippy::must_use_candidate, reason = "prost-generated code")]
    #![allow(clippy::derive_partial_eq_without_eq, reason = "prost-generated code")]
    tonic::include_proto!("user_messages");
}

use atuin_common::userio::log::{Message as DomainMessage, Severity as DomainSeverity};
pub use codegen::*;
pub use follow_reply::Event as FollowReplyEvent;
use thiserror::Error;

// The generated code finds `Lagged` here.
use crate::grpc::common::pb as common;

#[derive(Debug, Error)]
#[error("a message from the daemon has no severity this client knows: {0}")]
pub struct UnknownSeverityError(i32);

impl From<DomainMessage> for Message {
    fn from(message: DomainMessage) -> Self {
        let severity = match message.severity {
            DomainSeverity::Info => Severity::Info,
            DomainSeverity::Warn => Severity::Warn,
            DomainSeverity::Error => Severity::Error,
        };
        Self {
            severity: severity.into(),
            text: message.text,
        }
    }
}

impl TryFrom<Message> for DomainMessage {
    type Error = UnknownSeverityError;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        let severity = match Severity::try_from(message.severity) {
            Ok(Severity::Info) => DomainSeverity::Info,
            Ok(Severity::Warn) => DomainSeverity::Warn,
            Ok(Severity::Error) => DomainSeverity::Error,
            Ok(Severity::Unspecified) | Err(_) => {
                return Err(UnknownSeverityError(message.severity));
            }
        };
        Ok(Self {
            severity,
            text: message.text,
        })
    }
}
