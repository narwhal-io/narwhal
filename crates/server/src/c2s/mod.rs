// SPDX-License-Identifier: BSD-3-Clause

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use narwhal_modulator::OutboundPrivatePayload;

mod config;
mod listener;
mod stream;

pub mod conn;
pub mod router;

pub use config::Limits;
pub use config::{Config, ListenerConfig};
pub use listener::C2sListener;
pub use router::Router;
pub use stream::MaybeKtlsStream;

// Re-export C2sClient from narwhal-client crate
pub use narwhal_client::{C2sClient, C2sSessionExtraInfo};

use narwhal_protocol::{Message, ModDirectParameters};

/// Routes private payloads from the modulator to connected clients.
///
/// This function spawns an asynchronous task that listens for outbound private payloads
/// from the modulator (M2S) and routes them to the appropriate client connections.
/// Each payload is wrapped in a ModDirect message and sent to all specified target users.
///
/// # Arguments
///
/// * `m2s_payload_rx` - A broadcast receiver for [`OutboundPrivatePayload`] messages
///   coming from the modulator. Each payload contains the data to send and a list
///   of target usernames.
/// * `router` - The [`Router`] instance used to route messages to connected clients.
///   The router's local domain is used as the 'from' field in ModDirect messages.
///
/// # Returns
///
/// A tuple containing:
/// * `JoinHandle<()>` - A handle to the spawned routing task that can be used to
///   await its completion
/// * `CancellationToken` - A token that can be used to gracefully shut down the
///   routing task
///
/// # Implementation Details
///
/// The spawned task runs a loop that:
/// 1. Waits for incoming payloads from the modulator
/// 2. Creates a ModDirect message with the payload size and local domain
/// 3. Routes the message to each target user specified in the payload
/// 4. Continues until the cancellation token is triggered
pub fn route_m2s_private_payload(
  mut m2s_payload_rx: broadcast::Receiver<OutboundPrivatePayload>,
  router: Router,
) -> (tokio::task::JoinHandle<()>, CancellationToken) {
  let token = CancellationToken::new();
  let token_clone = token.clone();

  let handle = tokio::spawn(async move {
    loop {
      tokio::select! {
        Ok(outbound) = m2s_payload_rx.recv() => {
          let mod_priv_msg = Message::ModDirect(ModDirectParameters {
            id: None,
            from: router.local_domain(),
            length: outbound.payload.len() as u32,
          });
          for target in outbound.targets {
            match router.route_to(mod_priv_msg.clone(), Some(outbound.payload.clone()), target, None) {
              Ok(_) => {},
              Err(err) => {
                warn!("failed to route private payload: {}", err);
              }
            }
          }
        }
        _ = token_clone.cancelled() => {
          break;
        }
      }
    }
  });

  (handle, token)
}
