// SPDX-License-Identifier: BSD-3-Clause

pub mod metrics;

use std::io::stdout;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::anyhow;
use console_subscriber::ConsoleLayer;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Deserialize, Serialize};
use tracing::metadata::LevelFilter;
use tracing_subscriber::fmt;

/// Configuration for the telemetry system
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
  /// the logging configuration
  #[serde(default)]
  logging: LoggingConfig,

  /// the configuration for metrics
  #[serde(default)]
  pub metrics: MetricsConfig,

  /// the configuration for the console
  #[serde(default)]
  console: ConsoleConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
  /// whether metrics are enabled
  #[serde(default = "default_metrics_enabled")]
  pub enabled: bool,

  /// the port to expose Prometheus metrics on
  #[serde(default = "default_metrics_port")]
  pub port: u16,
}

impl Default for MetricsConfig {
  fn default() -> Self {
    Self { enabled: default_metrics_enabled(), port: default_metrics_port() }
  }
}

fn default_metrics_enabled() -> bool {
  true
}

fn default_metrics_port() -> u16 {
  9090
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsoleConfig {
  /// whether the console is enabled
  #[serde(default)]
  pub enabled: bool,

  /// the address to bind to
  #[serde(default = "default_bind_address")]
  pub bind: String,

  /// maximum capacity for the channel of events sent from a console layer to the server
  #[serde(default = "default_event_buffer_capacity")]
  pub event_buffer_capacity: usize,

  /// maximum capacity for the channel of events sent from the server to clients
  #[serde(default = "default_client_buffer_capacity")]
  pub client_buffer_capacity: usize,

  /// the frequency for publishing events to clients
  #[serde(default = "default_publish_interval", with = "humantime_serde")]
  pub publish_interval: Duration,

  /// the duration to retain spans
  #[serde(default = "default_retention", with = "humantime_serde")]
  pub retention: Duration,
}

impl Default for ConsoleConfig {
  fn default() -> Self {
    Self {
      enabled: false,
      bind: default_bind_address(),
      event_buffer_capacity: default_event_buffer_capacity(),
      client_buffer_capacity: default_client_buffer_capacity(),
      publish_interval: default_publish_interval(),
      retention: default_retention(),
    }
  }
}

fn default_bind_address() -> String {
  format!("{}:{}", console_subscriber::Server::DEFAULT_IP, console_subscriber::Server::DEFAULT_PORT)
}

fn default_event_buffer_capacity() -> usize {
  ConsoleLayer::DEFAULT_EVENT_BUFFER_CAPACITY
}

fn default_client_buffer_capacity() -> usize {
  ConsoleLayer::DEFAULT_CLIENT_BUFFER_CAPACITY
}

fn default_publish_interval() -> Duration {
  ConsoleLayer::DEFAULT_PUBLISH_INTERVAL
}

fn default_retention() -> Duration {
  ConsoleLayer::DEFAULT_RETENTION
}

/// Configuration for the logging system
#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingConfig {
  /// the logging level
  #[serde(default = "default_level")]
  level: String,

  /// the logging format
  #[serde(default = "default_format")]
  format: String,
}

impl Default for LoggingConfig {
  fn default() -> Self {
    Self { level: default_level(), format: default_format() }
  }
}

fn default_level() -> String {
  "info".to_string()
}

fn default_format() -> String {
  "text".to_string()
}

/// Initializes the telemetry system based on the provided configuration.
///
/// # Arguments
/// * `config` - The configuration for the telemetry system
///
/// # Returns
/// An error if the telemetry system could not be initialized
pub fn init(config: Config) -> anyhow::Result<()> {
  // Initialize the console subscriber
  let console_builder_opt = init_console(config.console)?;

  // Convert the string level to a tracing Level
  let level_filter = match config.logging.level.to_lowercase().as_str() {
    "trace" => LevelFilter::TRACE,
    "debug" => LevelFilter::DEBUG,
    "info" => LevelFilter::INFO,
    "warn" => LevelFilter::WARN,
    "error" => LevelFilter::ERROR,
    _ => return Err(anyhow!("invalid logging level: {}", config.logging.level)),
  };

  // Set up the tracing subscriber based on the format
  match config.logging.format.as_str() {
    "json" => init_json_logger(level_filter, console_builder_opt),
    "text" => init_text_logger(level_filter, console_builder_opt),
    _ => return Err(anyhow!("invalid logging format: {}", config.logging.format)),
  };

  // Initialize the metrics exporter
  init_metrics(config.metrics)?;

  Ok(())
}

fn init_console(config: ConsoleConfig) -> anyhow::Result<Option<console_subscriber::Builder>> {
  if !config.enabled {
    return Ok(None);
  }
  use std::str::FromStr;

  let socket_addr = std::net::SocketAddrV4::from_str(config.bind.as_str())?;

  let builder = ConsoleLayer::builder()
    .server_addr(socket_addr)
    .event_buffer_capacity(config.event_buffer_capacity)
    .client_buffer_capacity(config.client_buffer_capacity)
    .publish_interval(config.publish_interval)
    .retention(config.retention);

  Ok(Some(builder))
}

fn init_json_logger(level_filter: LevelFilter, console_builder_opt: Option<console_subscriber::Builder>) {
  use tracing_subscriber::prelude::*;

  let fmt_layer =
    fmt::Layer::new().json().with_target(false).with_timer(fmt::time::UtcTime::rfc_3339()).with_writer(stdout);

  let registry = tracing_subscriber::registry().with(fmt_layer.with_filter(level_filter));

  if let Some(console_builder) = console_builder_opt {
    let console_layer = console_builder.spawn();
    registry.with(console_layer).init();
  } else {
    registry.init();
  }
}

fn init_text_logger(level_filter: LevelFilter, console_builder_opt: Option<console_subscriber::Builder>) {
  use tracing_subscriber::prelude::*;

  let fmt_layer = fmt::Layer::new()
    .pretty()
    .with_target(false)
    .with_file(false)
    .with_line_number(false)
    .with_writer(stdout)
    .compact();

  let registry = tracing_subscriber::registry().with(fmt_layer.with_filter(level_filter));

  if let Some(console_builder) = console_builder_opt {
    let console_layer = console_builder.spawn();
    registry.with(console_layer).init();
  } else {
    registry.init();
  }
}

fn init_metrics(config: MetricsConfig) -> anyhow::Result<()> {
  if !config.enabled {
    return Ok(());
  }
  // Create bind address from configured port
  let bind_address: SocketAddr = format!("0.0.0.0:{}", config.port).parse()?;

  // Install the Prometheus exporter
  PrometheusBuilder::new().with_http_listener(bind_address).install()?;

  // Describe all metrics
  metrics::describe_metrics();

  Ok(())
}
