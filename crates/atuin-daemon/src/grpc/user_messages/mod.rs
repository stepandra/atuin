//! What the daemon would tell the user, streamed to the clients following it.

pub mod pb;

use std::pin::Pin;

use atuin_common::userio::log::{Message, Sink, Stderr};
use futures::StreamExt as _;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, SendError};
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::common::pb::Lagged;
use crate::grpc::user_messages::pb::user_messages_server::UserMessages as GrpcService;
use crate::grpc::user_messages::pb::{FollowReply, FollowReplyEvent, FollowRequest};

/// The daemon's sink, which has no terminal: it sends each message to the clients following it, to
/// show to their user.
#[derive(Clone)]
pub struct Service {
    messages: broadcast::Sender<Message>,
}

impl Service {
    /// How many messages a client may fall behind by before it misses some.
    const BACKLOG: usize = 64;
}

impl Default for Service {
    fn default() -> Self {
        Self {
            messages: broadcast::channel(Self::BACKLOG).0,
        }
    }
}

impl Sink for Service {
    fn show(&self, message: Message) {
        // While no client follows, the message goes where the service manager keeps the daemon's
        // stderr.
        if let Err(SendError(message)) = self.messages.send(message) {
            Stderr::default().show(message);
        }
    }
}

#[tonic::async_trait]
impl GrpcService for Service {
    type FollowStream = Pin<Box<dyn Stream<Item = Result<FollowReply, Status>> + Send>>;

    async fn follow(
        &self,
        request: Request<Streaming<FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let mut messages = self.messages.subscribe();
        let mut requests = request.into_inner();

        let replies = async_stream::stream! {
            loop {
                let event = tokio::select! {
                    // Messages first, so every one shown before the client closed its side is sent
                    // before the stream ends.
                    biased;
                    message = messages.recv() => match message {
                        Ok(message) => FollowReplyEvent::Message(message.into()),
                        Err(RecvError::Lagged(dropped)) => FollowReplyEvent::Lagged(Lagged { dropped }),
                        Err(RecvError::Closed) => break,
                    },
                    request = requests.next() => match request {
                        Some(Ok(FollowRequest {})) => continue,
                        Some(Err(_)) | None => break,
                    },
                };
                yield Ok(FollowReply { event: Some(event) });
            }
        };

        Ok(Response::new(Box::pin(replies)))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::Arc;

    use atuin_common::userio::log::{Message, Severity, Sink, SinkStack};
    use parking_lot::Mutex;
    use rstest::rstest;
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::transport::Server;

    use super::Service;
    use crate::client::UserMessages;
    use crate::grpc::user_messages::pb::user_messages_server::UserMessagesServer;
    use crate::service::address::DaemonAddress;

    /// Keeps the messages shown on it.
    #[derive(Clone, Default)]
    struct Shown(Arc<Mutex<Vec<Message>>>);

    impl Sink for Shown {
        fn show(&self, message: Message) {
            self.0.lock().push(message);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn a_client_shows_every_message_the_daemon_showed_before_it_finished(
        #[values(1, Service::BACKLOG)] count: usize,
    ) {
        static STACK: SinkStack = SinkStack::new();
        let shown = Shown::default();
        let _shown = STACK.push(shown.clone());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.sock");
        let service = Service::default();
        let incoming = UnixListenerStream::new(UnixListener::bind(&path).unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(UserMessagesServer::new(service.clone()))
                .serve_with_incoming(incoming),
        );

        let channel = DaemonAddress::from(path).channel().await.unwrap();
        let messages = UserMessages::follow(channel, &STACK).await.unwrap();
        let sent: Vec<_> = (0..count)
            .map(|i| Message {
                severity: Severity::Warn,
                text: format!("message {i}"),
            })
            .collect();
        for message in &sent {
            service.show(message.clone());
        }
        messages.finish().await;

        assert_eq!(*shown.0.lock(), sent);
    }
}
