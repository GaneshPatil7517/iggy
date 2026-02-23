/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(InfluxDbSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_PRECISION: &str = "ms";

#[derive(Debug)]
pub struct InfluxDbSink {
    pub id: u32,
    config: InfluxDbSinkConfig,
    client: Option<Client>,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDbSinkConfig {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
    pub measurement: Option<String>,
    pub precision: Option<String>,
    pub batch_size: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub include_stream_tag: Option<bool>,
    pub include_topic_tag: Option<bool>,
    pub include_partition_tag: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PayloadFormat {
    #[default]
    Json,
    Text,
    Base64,
}

impl PayloadFormat {
    fn from_config(value: Option<&str>) -> Self {
        match value.map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("text") | Some("utf8") => PayloadFormat::Text,
            Some("base64") | Some("raw") => PayloadFormat::Base64,
            _ => PayloadFormat::Json,
        }
    }
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    write_errors: u64,
}

impl InfluxDbSink {
    pub fn new(id: u32, config: InfluxDbSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);

        InfluxDbSink {
            id,
            config,
            client: None,
            state: Mutex::new(State {
                messages_processed: 0,
                write_errors: 0,
            }),
            verbose,
            retry_delay,
        }
    }

    fn build_client(&self) -> Result<Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn build_write_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        let mut url = Url::parse(&format!("{base}/api/v2/write"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        let precision = self
            .config
            .precision
            .as_deref()
            .unwrap_or(DEFAULT_PRECISION);
        url.query_pairs_mut()
            .append_pair("org", &self.config.org)
            .append_pair("bucket", &self.config.bucket)
            .append_pair("precision", precision);

        Ok(url)
    }

    fn build_health_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }

    async fn check_connectivity(&self) -> Result<(), Error> {
        let client = self.get_client()?;
        let url = self.build_health_url()?;

        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("InfluxDB health check failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read response body".to_string());
            return Err(Error::Connection(format!(
                "InfluxDB health check returned status {status}: {body}"
            )));
        }

        Ok(())
    }

    fn get_client(&self) -> Result<&Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client is not initialized".to_string()))
    }

    fn measurement(&self) -> &str {
        self.config
            .measurement
            .as_deref()
            .unwrap_or("iggy_messages")
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn timestamp_precision(&self) -> &str {
        self.config
            .precision
            .as_deref()
            .unwrap_or(DEFAULT_PRECISION)
    }

    fn get_max_retries(&self) -> u32 {
        self.config
            .max_retries
            .unwrap_or(DEFAULT_MAX_RETRIES)
            .max(1)
    }

    fn to_precision_timestamp(&self, millis: u64) -> u64 {
        match self.timestamp_precision() {
            "ns" => millis.saturating_mul(1_000_000),
            "us" => millis.saturating_mul(1_000),
            "s" => millis / 1_000,
            _ => millis,
        }
    }

    fn line_from_message(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &ConsumedMessage,
    ) -> Result<String, Error> {
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);

        let mut tags = Vec::new();
        if include_metadata && self.config.include_stream_tag.unwrap_or(true) {
            tags.push(format!(
                "stream={}",
                escape_tag_value(&topic_metadata.stream)
            ));
        }
        if include_metadata && self.config.include_topic_tag.unwrap_or(true) {
            tags.push(format!("topic={}", escape_tag_value(&topic_metadata.topic)));
        }
        if include_metadata && self.config.include_partition_tag.unwrap_or(true) {
            tags.push(format!("partition={}", messages_metadata.partition_id));
        }

        let mut fields = vec![
            format!(
                "message_id=\"{}\"",
                escape_field_string(&message.id.to_string())
            ),
            format!("offset={}i", message.offset as i64),
        ];

        if include_metadata && !self.config.include_stream_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_stream=\"{}\"",
                escape_field_string(&topic_metadata.stream)
            ));
        }
        if include_metadata && !self.config.include_topic_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_topic=\"{}\"",
                escape_field_string(&topic_metadata.topic)
            ));
        }
        if include_metadata && !self.config.include_partition_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_partition={}i",
                messages_metadata.partition_id as i64
            ));
        }

        if include_checksum {
            fields.push(format!("iggy_checksum={}i", message.checksum as i64));
        }
        if include_origin_timestamp {
            fields.push(format!(
                "iggy_origin_timestamp={}i",
                message.origin_timestamp as i64
            ));
        }

        let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
            Error::CannotStoreData(format!("Failed to convert payload to bytes: {e}"))
        })?;

        match self.payload_format() {
            PayloadFormat::Json => {
                let value: serde_json::Value =
                    serde_json::from_slice(&payload_bytes).map_err(|e| {
                        Error::CannotStoreData(format!(
                            "Payload format is json but payload is invalid JSON: {e}"
                        ))
                    })?;
                let compact = serde_json::to_string(&value).map_err(|e| {
                    Error::CannotStoreData(format!("Failed to serialize JSON payload: {e}"))
                })?;
                fields.push(format!(
                    "payload_json=\"{}\"",
                    escape_field_string(&compact)
                ));
            }
            PayloadFormat::Text => {
                let text = String::from_utf8(payload_bytes).map_err(|e| {
                    Error::CannotStoreData(format!(
                        "Payload format is text but payload is invalid UTF-8: {e}"
                    ))
                })?;
                fields.push(format!("payload_text=\"{}\"", escape_field_string(&text)));
            }
            PayloadFormat::Base64 => {
                let encoded = general_purpose::STANDARD.encode(payload_bytes);
                fields.push(format!(
                    "payload_base64=\"{}\"",
                    escape_field_string(&encoded)
                ));
            }
        }

        let measurement = escape_measurement(self.measurement());
        let tags_fragment = if tags.is_empty() {
            String::new()
        } else {
            format!(",{}", tags.join(","))
        };

        let ts = self.to_precision_timestamp(message.timestamp);
        Ok(format!(
            "{measurement}{tags_fragment} {} {ts}",
            fields.join(",")
        ))
    }

    async fn process_batch(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut lines = Vec::with_capacity(messages.len());
        for message in messages {
            lines.push(self.line_from_message(topic_metadata, messages_metadata, message)?);
        }

        let body = lines.join("\n");
        self.write_with_retry(body).await
    }

    async fn write_with_retry(&self, body: String) -> Result<(), Error> {
        let client = self.get_client()?;
        let url = self.build_write_url()?;
        let max_retries = self.get_max_retries();
        let token = self.config.token.clone();

        let mut attempts = 0;
        loop {
            let response_result = client
                .post(url.clone())
                .header("Authorization", format!("Token {token}"))
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body.clone())
                .send()
                .await;

            match response_result {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::NO_CONTENT || status == StatusCode::OK {
                        return Ok(());
                    }

                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response body".to_string());

                    attempts += 1;
                    if is_transient_status(status) && attempts < max_retries {
                        warn!(
                            "Transient InfluxDB write error (attempt {attempts}/{max_retries}): {status}. Retrying..."
                        );
                        tokio::time::sleep(self.retry_delay * attempts).await;
                        continue;
                    }

                    return Err(Error::CannotStoreData(format!(
                        "InfluxDB write failed with status {status}: {body}"
                    )));
                }
                Err(e) => {
                    attempts += 1;
                    if attempts < max_retries {
                        warn!(
                            "Failed to send write request to InfluxDB (attempt {attempts}/{max_retries}): {e}. Retrying..."
                        );
                        tokio::time::sleep(self.retry_delay * attempts).await;
                        continue;
                    }

                    return Err(Error::CannotStoreData(format!(
                        "InfluxDB write failed after {attempts} attempts: {e}"
                    )));
                }
            }
        }
    }
}

#[async_trait]
impl Sink for InfluxDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening InfluxDB sink connector with ID: {}. Bucket: {}, org: {}",
            self.id, self.config.bucket, self.config.org
        );

        self.client = Some(self.build_client()?);
        self.check_connectivity().await?;

        info!(
            "InfluxDB sink connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let batch_size = self.config.batch_size.unwrap_or(500) as usize;

        for batch in messages.chunks(batch_size.max(1)) {
            if let Err(e) = self
                .process_batch(topic_metadata, &messages_metadata, batch)
                .await
            {
                let mut state = self.state.lock().await;
                state.write_errors += batch.len() as u64;
                error!("Failed to write batch to InfluxDB: {e}");
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;

        if self.verbose {
            info!(
                "InfluxDB sink ID: {} processed {} messages. Total processed: {}, write errors: {}",
                self.id,
                messages.len(),
                state.messages_processed,
                state.write_errors
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} processed {} messages. Total processed: {}, write errors: {}",
                self.id,
                messages.len(),
                state.messages_processed,
                state.write_errors
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "InfluxDB sink connector with ID: {} closed. Processed: {}, errors: {}",
            self.id, state.messages_processed, state.write_errors
        );
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(|d| d.into())
        .unwrap_or_else(|_| Duration::from_secs(1))
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn escape_measurement(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace(' ', "\\ ")
}

fn escape_tag_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

fn escape_field_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
