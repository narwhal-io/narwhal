// SPDX-License-Identifier: BSD-3-Clause

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;

use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use async_channel::Receiver;
use narwhal_common::client::{self, Handshaker, SessionInfo};
use narwhal_common::service::C2sService;
use narwhal_protocol::{
  AclAction, AclType, AuthParameters, ConnectParameters, DEFAULT_MESSAGE_BUFFER_SIZE, IdentifyParameters, Message, Nid,
  QoS, request,
};
use narwhal_util::conn::TlsDialer;
use narwhal_util::pool::{Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

/// Configuration for C2S connections.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct C2sConfig {
  /// The address of the server to connect to.
  #[serde(default)]
  pub address: String,

  /// The heartbeat interval for the client that should be negotiated
  /// with the server.
  #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
  pub heartbeat_interval: Duration,

  /// The client connection timeout.
  /// This is the timeout for establishing a connection to the server.
  #[serde(default = "default_connect_timeout", with = "humantime_serde")]
  pub connect_timeout: Duration,

  /// The client read/write timeout.
  #[serde(default = "default_timeout", with = "humantime_serde")]
  pub timeout: Duration,

  /// The timeout for reading a payload from the server.
  #[serde(default = "default_payload_read_timeout", with = "humantime_serde")]
  pub payload_read_timeout: Duration,

  /// The initial delay for the backoff strategy.
  #[serde(default = "default_backoff_initial_delay", with = "humantime_serde")]
  pub backoff_initial_delay: Duration,

  /// The maximum delay for the backoff strategy.
  #[serde(default = "default_backoff_max_delay", with = "humantime_serde")]
  pub backoff_max_delay: Duration,

  /// The maximum number of retries for the backoff strategy.
  #[serde(default = "default_backoff_max_retries")]
  pub backoff_max_retries: usize,
}

impl From<C2sConfig> for narwhal_common::client::Config {
  fn from(val: C2sConfig) -> Self {
    narwhal_common::client::Config {
      max_idle_connections: 1,
      heartbeat_interval: val.heartbeat_interval,
      connect_timeout: val.connect_timeout,
      timeout: val.timeout,
      payload_read_timeout: val.payload_read_timeout,
      backoff_initial_delay: val.backoff_initial_delay,
      backoff_max_delay: val.backoff_max_delay,
      backoff_max_retries: val.backoff_max_retries,
    }
  }
}

fn default_heartbeat_interval() -> Duration {
  Duration::from_secs(60)
}

fn default_connect_timeout() -> Duration {
  Duration::from_secs(5)
}

fn default_timeout() -> Duration {
  Duration::from_secs(5)
}

fn default_payload_read_timeout() -> Duration {
  Duration::from_secs(5)
}

fn default_backoff_initial_delay() -> Duration {
  Duration::from_millis(100)
}

fn default_backoff_max_delay() -> Duration {
  Duration::from_secs(30)
}

fn default_backoff_max_retries() -> usize {
  5
}

/// A trait for implementing multi-step authentication mechanisms.
///
/// This trait supports SASL-style authentication protocols where the client
/// and server exchange multiple tokens/challenges until authentication completes.
/// Implementations should maintain internal state across the authentication flow.
#[async_trait::async_trait]
pub trait Authenticator: Send {
  /// Initiates the authentication process.
  ///
  /// Returns the initial authentication token to send to the server.
  /// This is called once at the beginning of the authentication flow.
  ///
  /// # Errors
  ///
  /// Returns an error if the authenticator fails to generate the initial token.
  async fn start(&mut self) -> anyhow::Result<String>;

  /// Processes a server challenge and generates the next response.
  ///
  /// This method is called for each challenge received from the server during
  /// the authentication flow. The implementation should process the challenge
  /// and return an appropriate response token.
  ///
  /// # Arguments
  ///
  /// * `challenge` - The challenge token received from the server
  ///
  /// # Errors
  ///
  /// Returns an error if the challenge cannot be processed or if the
  /// authentication flow fails.
  async fn next(&mut self, challenge: String) -> anyhow::Result<String>;
}

/// A factory trait for creating new `Authenticator` instances.
///
/// This trait is used to create fresh authenticator instances for each connection
/// attempt. Since `Authenticator` implementations are stateful (maintaining state
/// across the multi-step authentication flow), a factory is needed to produce new
/// instances for reconnections or retry attempts.
pub trait AuthenticatorFactory: Send + Sync + 'static {
  /// Creates a new `Authenticator` instance.
  ///
  /// This method is called each time a new authentication flow needs to begin,
  /// ensuring that each attempt starts with a fresh authenticator state.
  ///
  /// # Returns
  ///
  /// Returns a boxed `Authenticator` ready to begin the authentication process.
  fn create(&self) -> Box<dyn Authenticator>;
}

/// Authentication method to use when connecting to the server.
///
/// Supports two authentication approaches:
/// - Simple username-based identification (IDENTIFY command)
/// - Multi-step authentication with custom authenticator (AUTH command)
#[derive(Clone)]
pub enum AuthMethod {
  /// Simple username-based authentication using the IDENTIFY command.
  ///
  /// This is a single-step authentication where only a username is required.
  Identify { username: String },

  /// Multi-step authentication using a custom authenticator factory.
  ///
  /// This supports SASL-style authentication protocols where multiple
  /// challenge-response exchanges may occur. The factory creates fresh
  /// authenticator instances for each connection attempt, ensuring that
  /// reconnections start with clean state.
  Auth { authenticator_factory: Arc<dyn AuthenticatorFactory> },
}

impl std::fmt::Debug for AuthMethod {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      AuthMethod::Identify { username } => write!(f, "Identify({})", username),
      AuthMethod::Auth { .. } => write!(f, "Auth"),
    }
  }
}

/// Session information returned after successful C2S handshake.
#[derive(Clone, Debug)]
pub struct C2sSessionExtraInfo {
  pub nid: Nid,
}

/// Handshaker implementation for C2S connections.
#[derive(Clone)]
struct C2sHandshaker {
  /// The requested heartbeat interval.
  heartbeat_interval: Duration,

  /// The username to use for identification.
  username: Option<String>,

  /// The authenticator factory to use for authentication (when required).
  authenticator_factory: Option<Arc<dyn AuthenticatorFactory>>,
}

// === impl C2sHandshaker ===

impl std::fmt::Debug for C2sHandshaker {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("C2sHandshaker")
      .field("heartbeat_interval", &self.heartbeat_interval)
      .field("username", &self.username.is_some())
      .field("authenticator_factory", &self.authenticator_factory.is_some())
      .finish()
  }
}

#[async_trait::async_trait]
impl Handshaker<TlsStream<TcpStream>> for C2sHandshaker {
  type SessionExtraInfo = C2sSessionExtraInfo;

  async fn handshake(&self, stream: &mut TlsStream<TcpStream>) -> anyhow::Result<(SessionInfo, C2sSessionExtraInfo)> {
    let mut pool = Pool::new(1, DEFAULT_MESSAGE_BUFFER_SIZE);
    let mut message_buff = pool.acquire_buffer().await;

    let connect_msg = Message::Connect(ConnectParameters {
      protocol_version: 1,
      heartbeat_interval: self.heartbeat_interval.as_millis() as u32,
    });

    let connect_ack_msg = request(connect_msg, stream, message_buff).await?;

    let (auth_required, session_info) = match connect_ack_msg {
      Message::ConnectAck(params) => {
        let session_info = SessionInfo {
          heartbeat_interval: params.heartbeat_interval,
          max_inflight_requests: params.max_inflight_requests,
          max_message_size: params.max_message_size,
          max_payload_size: params.max_payload_size,
        };

        (params.auth_required, session_info)
      },
      Message::Error(err) => {
        return Err(anyhow!("connection rejected: {:?}", err.reason));
      },
      _ => {
        return Err(anyhow!("unexpected message during handshake: expected ConnectAck"));
      },
    };

    pool = Pool::new(1, session_info.max_message_size as usize);
    message_buff = pool.acquire_buffer().await;

    let nid: Nid;

    if !auth_required && let Some(username) = self.username.as_ref() {
      let identify_msg = Message::Identify(IdentifyParameters { username: username.as_str().into() });

      match request(identify_msg, stream, message_buff).await? {
        Message::IdentifyAck(params) => nid = Nid::try_from(params.nid)?,
        Message::Error(err) => return Err(anyhow!("error during handshake: {:?}", err.reason)),
        _ => return Err(anyhow!("unexpected message during handshake: expected IdentifyAck")),
      }
    } else if let Some(auth_factory) = self.authenticator_factory.as_ref() {
      let mut authenticator = auth_factory.create();

      // Start authentication flow
      let mut token = authenticator.start().await?;

      loop {
        let auth_msg = Message::Auth(AuthParameters { token: token.as_str().into() });

        match request(auth_msg, stream, message_buff).await? {
          Message::AuthAck(params) => {
            if let Some(succeded) = params.succeeded
              && let Some(nid_str) = params.nid
              && succeded
            {
              // Authentication succeeded
              nid = Nid::try_from(nid_str)?;
              break;
            } else if let Some(challenge) = params.challenge {
              // Continue with challenge-response
              token = authenticator.next(challenge.to_string()).await?;
              message_buff = pool.acquire_buffer().await;
            } else {
              // Authentication failed
              return Err(anyhow!("authentication failed"));
            }
          },
          Message::Error(err) => {
            return Err(anyhow!("error during authentication: {:?}", err.reason));
          },
          _ => {
            return Err(anyhow!("unexpected message during authentication: expected AuthAck"));
          },
        }
      }
    } else {
      return Err(anyhow!("no proper authentication method provided"));
    }

    Ok((session_info, C2sSessionExtraInfo { nid }))
  }
}

impl C2sHandshaker {
  /// Creates a new handshaker with the specified heartbeat interval and authentication method.
  ///
  /// # Arguments
  ///
  /// * `heartbeat_interval` - The heartbeat interval to negotiate with the server
  /// * `auth_method` - The authentication method to use during handshake
  ///
  /// # Returns
  ///
  /// Returns a new `C2sHandshaker` instance configured with the provided parameters.
  fn new(heartbeat_interval: Duration, auth_method: AuthMethod) -> Self {
    match auth_method {
      AuthMethod::Identify { username } => {
        Self { heartbeat_interval, username: Some(username), authenticator_factory: None }
      },
      AuthMethod::Auth { authenticator_factory } => {
        Self { heartbeat_interval, username: None, authenticator_factory: Some(authenticator_factory) }
      },
    }
  }
}

/// C2S client for connecting to the Narwhal server.
#[derive(Clone)]
pub struct C2sClient {
  client: Arc<client::Client<TlsStream<TcpStream>, C2sHandshaker, C2sService>>,
}

impl C2sClient {
  /// Creates a new C2S client instance.
  ///
  /// This method initializes a client that connects to the Narwhal server, handling
  /// the handshake process.
  ///
  /// By default, this method enables TLS certificate verification for secure connections.
  /// For testing with self-signed certificates, use `new_with_insecure_tls()` instead.
  ///
  /// # Arguments
  ///
  /// * `config` - The client configuration containing the server address, network settings,
  ///   timeouts, heartbeat intervals, and other connection parameters.
  /// * `auth_method` - The authentication method to use when connecting to the server.
  ///   Can be either simple username-based identification or multi-step authentication.
  ///
  /// # Returns
  ///
  /// Returns a `C2sClient` instance that can be used to communicate with the Narwhal server.
  pub fn new(config: C2sConfig, auth_method: AuthMethod) -> anyhow::Result<Self> {
    let dialer = Arc::new(TlsDialer::new(config.address.as_str().into())?);

    let handshaker = C2sHandshaker::new(config.heartbeat_interval, auth_method);

    let client = Arc::new(client::Client::new("c2s-client", config.into(), dialer, handshaker)?);

    Ok(Self { client })
  }

  /// Creates a new C2S (Client-to-Server) client instance with insecure TLS.
  ///
  /// # Security Warning
  ///
  /// **DANGER**: This method disables TLS certificate verification, making the connection
  /// vulnerable to man-in-the-middle attacks. This should **ONLY** be used in
  /// development/testing environments with self-signed certificates. **NEVER** use this
  /// in production environments.
  ///
  /// # Arguments
  ///
  /// * `config` - The client configuration containing the server address, network settings,
  ///   timeouts, heartbeat intervals, and other connection parameters.
  /// * `auth_method` - The authentication method to use when connecting to the server.
  ///   Can be either simple username-based identification or multi-step authentication.
  ///
  /// # Returns
  ///
  /// Returns a `C2sClient` instance that can be used to communicate with the Narwhal server.
  pub fn new_with_insecure_tls(config: C2sConfig, auth_method: AuthMethod) -> anyhow::Result<Self> {
    let dialer = Arc::new(TlsDialer::with_certificate_verification(config.address.as_str().into(), false)?);

    let handshaker = C2sHandshaker::new(config.heartbeat_interval, auth_method);

    let client = Arc::new(client::Client::new("c2s-client", config.into(), dialer, handshaker)?);

    Ok(Self { client })
  }

  /// Retrieves the session information established during the handshake.
  ///
  /// This method returns the session details negotiated with the server during
  /// the connection handshake. The information includes configuration limits and
  /// parameters that govern the client-server communication.
  ///
  /// # Returns
  ///
  /// Returns session information including:
  /// * `heartbeat_interval` - The negotiated heartbeat interval in milliseconds
  /// * `max_inflight_requests` - Maximum number of concurrent in-flight requests
  /// * `max_message_size` - Maximum size of a message in bytes
  /// * `max_payload_size` - Maximum size of a payload in bytes
  /// * `extra` - Additional C2S-specific session info
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The client is not connected to the server
  /// * The session information is not available
  pub async fn session_info(&self) -> anyhow::Result<(SessionInfo, C2sSessionExtraInfo)> {
    self.client.session_info().await
  }

  /// Joins a channel on the server.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to join.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully joined.
  ///
  /// # Errors
  ///
  /// Returns an error if the join operation fails.
  pub async fn join_channel(&self, channel: StringAtom) -> anyhow::Result<()> {
    use narwhal_protocol::JoinChannelParameters;

    let id = self.client.next_id().await;
    let message = Message::JoinChannel(JoinChannelParameters { id, channel, on_behalf: None });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::JoinChannelAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to join channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to join channel request")),
    }
  }

  /// Leaves a channel on the server.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to leave.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully left.
  ///
  /// # Errors
  ///
  /// Returns an error if the leave operation fails.
  pub async fn leave_channel(&self, channel: StringAtom) -> anyhow::Result<()> {
    use narwhal_protocol::LeaveChannelParameters;

    let id = self.client.next_id().await;
    let message = Message::LeaveChannel(LeaveChannelParameters { id, channel, on_behalf: None });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::LeaveChannelAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to leave channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to leave channel request")),
    }
  }

  /// Configures channel settings such as maximum clients and payload size.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to configure.
  /// * `max_clients` - The maximum number of clients allowed in the channel.
  /// * `max_payload_size` - The maximum payload size (in bytes) for messages in the channel.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully configured.
  ///
  /// # Errors
  ///
  /// Returns an error if the configuration operation fails or the user is not
  /// authorized to configure the channel.
  pub async fn configure_channel(
    &self,
    channel: StringAtom,
    max_clients: u32,
    max_payload_size: u32,
  ) -> anyhow::Result<()> {
    use narwhal_protocol::SetChannelConfigurationParameters;

    let id = self.client.next_id().await;
    let message = Message::SetChannelConfiguration(SetChannelConfigurationParameters {
      id,
      channel,
      max_clients,
      max_payload_size,
    });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::SetChannelConfigurationAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to configure channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to configure channel request")),
    }
  }

  /// Sets the access control list (ACL) for a channel.
  ///
  /// This method configures who can join, publish to, and read from a channel.
  /// Only the channel owner or authorized users can set channel ACLs.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to configure.
  /// * `allow_join` - List of user IDs or domains allowed to join the channel.
  /// * `allow_publish` - List of user IDs or domains allowed to publish to the channel.
  /// * `allow_read` - List of user IDs or domains allowed to read from the channel.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the ACL was successfully set.
  ///
  /// # Errors
  ///
  /// Returns an error if the ACL operation fails or the user is not
  /// authorized to modify the channel's ACL.
  pub async fn set_channel_acl(
    &self,
    channel: StringAtom,
    acl_type: AclType,
    acl_action: AclAction,
    nids: Vec<Nid>,
  ) -> anyhow::Result<()> {
    use narwhal_protocol::SetChannelAclParameters;

    let id = self.client.next_id().await;
    let message = Message::SetChannelAcl(SetChannelAclParameters {
      id,
      channel,
      nids: nids.iter().map(|nid| nid.into()).collect(),
      r#type: acl_type.as_str().into(),
      action: acl_action.as_str().into(),
    });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::SetChannelAclAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to set channel ACL: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to set channel ACL request")),
    }
  }

  /// Broadcasts a message to a channel.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to broadcast to.
  /// * `qos` - The Quality of Service level for the broadcast.
  /// * `payload` - The payload data to broadcast.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the broadcast was successful.
  ///
  /// # Errors
  ///
  /// Returns an error if the broadcast operation fails.
  pub async fn broadcast(&self, channel: StringAtom, qos: Option<QoS>, payload: PoolBuffer) -> anyhow::Result<()> {
    use narwhal_protocol::BroadcastParameters;

    let protocol_qos = qos.map(|q| q.as_u8());

    let id = self.client.next_id().await;
    let length = payload.len() as u32;
    let message = Message::Broadcast(BroadcastParameters { id, channel, qos: protocol_qos, length });

    let handle = self.client.send_message(message, Some(payload)).await?;
    let (response, _) = handle.await??;

    match response {
      Message::BroadcastAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to broadcast: reason={}, detail={:?}", err.reason, err.detail)),
      _ => Err(anyhow!("unexpected response to broadcast request")),
    }
  }

  /// Shuts down the client and closes all connections.
  ///
  /// This method gracefully shuts down the client, closing all active connections
  /// and cleaning up resources.
  ///
  /// # Errors
  ///
  /// Returns an error if the shutdown process fails.
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    self.client.shutdown().await
  }

  /// Returns a stream of inbound messages from the server.
  ///
  /// This method provides access to unsolicited messages sent by the server that are not
  /// responses to client requests. These are typically broadcast messages from channels
  /// the client has joined.
  ///
  /// # Important
  ///
  /// This method can only be called **once** per `Client` instance. Subsequent calls will panic.
  /// This is because the method takes ownership of the internal receiver, ensuring there is only
  /// one consumer of inbound messages.
  ///
  /// # Panics
  ///
  /// Panics if called more than once on the same `Client` instance.
  pub async fn inbound_stream(&self) -> Receiver<(Message, Option<PoolBuffer>)> {
    self.client.inbound_stream().await
  }
}
