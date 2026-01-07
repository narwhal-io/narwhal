// SPDX-License-Identifier: BSD-3-Clause

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// Metric names used throughout the server
pub mod names {
  /// Total number of c2s clients currently connected
  pub const C2S_CLIENTS_CONNECTED: &str = "narwhal_c2s_clients_connected";

  /// Total number of channels
  pub const TOTAL_CHANNELS: &str = "narwhal_channels_total";

  /// Number of inflight requests currently being processed
  pub const INFLIGHT_REQUESTS: &str = "narwhal_requests_inflight";

  /// Total number of requests processed
  pub const REQUESTS_TOTAL: &str = "narwhal_requests_total";

  /// Request elapsed time in milliseconds
  pub const REQUEST_DURATION: &str = "narwhal_request_duration_ms";
}

/// Set the total number of clients connected
pub fn set_c2s_clients_connected(count: f64) {
  gauge!(names::C2S_CLIENTS_CONNECTED).set(count);
}

/// Set the total number of channels
pub fn set_total_channels(count: f64) {
  gauge!(names::TOTAL_CHANNELS).set(count);
}

/// Increment the number of inflight requests
pub fn request_started(service_type: &'static str) {
  gauge!(names::INFLIGHT_REQUESTS, "service_type" => service_type).increment(1.0);
}

/// Decrement the number of inflight requests
pub fn request_finished(service_type: &'static str) {
  gauge!(names::INFLIGHT_REQUESTS, "service_type" => service_type).decrement(1.0);
}

/// Increment the total request counter
pub fn request_processed(service_type: &'static str) {
  counter!(names::REQUESTS_TOTAL, "service_type" => service_type).increment(1);
}

/// Record request duration in milliseconds
pub fn record_request_duration(duration_ms: f64, service_type: &'static str) {
  histogram!(names::REQUEST_DURATION, "service_type" => service_type).record(duration_ms);
}

/// Describes all metrics (should be called after installing the exporter)
pub fn describe_metrics() {
  describe_gauge!(names::C2S_CLIENTS_CONNECTED, "Total number of clients currently connected");
  describe_gauge!(names::TOTAL_CHANNELS, "Total number of channels");
  describe_gauge!(names::INFLIGHT_REQUESTS, "Number of inflight requests currently being processed");
  describe_counter!(names::REQUESTS_TOTAL, "Total number of requests processed");
  describe_histogram!(names::REQUEST_DURATION, "Request elapsed time in milliseconds");
}
