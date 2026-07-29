// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Minimal trace-seeded inference replay runner.
//!
//! Fixtures contain only allowlisted workload structure. Request and response content is generated
//! from fixed templates so source trace strings cannot cross into the repository.

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_STEP_TIMEOUT_MS: u64 = 30_000;
const MAX_SCENARIO_TIMEOUT_MS: u64 = 60_000;
const RATE_LIMIT_BODY: &str =
    r#"{"type":"error","error":{"type":"rate_limit_error","message":"synthetic rate limit"}}"#;
const SUCCESS_BODY: &str = concat!(
    "event: message_start\n",
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_synthetic\",",
    "\"type\":\"message\",\"role\":\"assistant\",\"model\":\"synthetic-model\",",
    "\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":",
    "{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":",
    "{\"type\":\"text_delta\",\"text\":\"synthetic-output\"}}\n\n",
    "event: content_block_stop\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",",
    "\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\n",
    "event: message_stop\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

fn gateway_bin() -> &'static str {
    env!("CARGO_BIN_EXE_nemo-relay")
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
enum ScenarioId {
    #[serde(rename = "streaming-429-followup")]
    Streaming429Followup,
}

impl ScenarioId {
    fn label(self) -> &'static str {
        match self {
            Self::Streaming429Followup => "streaming-429-followup",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderSurface {
    AnthropicMessages,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RequestTemplate {
    SyntheticStream,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderResponse {
    RateLimitJsonKeepAlive,
    AnthropicSseSuccessClose,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExpectedOutcome {
    RateLimitPassthrough,
    StreamSuccess,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Step {
    request: RequestTemplate,
    provider_response: ProviderResponse,
    expected: ExpectedOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    schema_version: u8,
    scenario_id: ScenarioId,
    seed: u64,
    provider_surface: ProviderSurface,
    step_timeout_ms: u64,
    scenario_timeout_ms: u64,
    require_upstream_connection_reuse: bool,
    steps: Vec<Step>,
}

impl Scenario {
    fn parse(source: &str) -> Result<Self, String> {
        let scenario = serde_json::from_str::<Self>(source)
            .map_err(|error| format!("invalid replay fixture: {error}"))?;
        scenario.validate()?;
        Ok(scenario)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported replay schema version {}",
                self.schema_version
            ));
        }
        if self.step_timeout_ms == 0 || self.step_timeout_ms > MAX_STEP_TIMEOUT_MS {
            return Err(format!(
                "step_timeout_ms must be between 1 and {MAX_STEP_TIMEOUT_MS}"
            ));
        }
        if self.scenario_timeout_ms == 0 || self.scenario_timeout_ms > MAX_SCENARIO_TIMEOUT_MS {
            return Err(format!(
                "scenario_timeout_ms must be between 1 and {MAX_SCENARIO_TIMEOUT_MS}"
            ));
        }
        if self.scenario_timeout_ms <= self.step_timeout_ms {
            return Err("scenario_timeout_ms must exceed step_timeout_ms".into());
        }
        if self.provider_surface != ProviderSurface::AnthropicMessages {
            return Err("the MVP supports only the Anthropic Messages surface".into());
        }
        if !self.require_upstream_connection_reuse {
            return Err("the MVP scenario must require upstream connection reuse".into());
        }
        let expected_steps = [
            Step {
                request: RequestTemplate::SyntheticStream,
                provider_response: ProviderResponse::RateLimitJsonKeepAlive,
                expected: ExpectedOutcome::RateLimitPassthrough,
            },
            Step {
                request: RequestTemplate::SyntheticStream,
                provider_response: ProviderResponse::AnthropicSseSuccessClose,
                expected: ExpectedOutcome::StreamSuccess,
            },
        ];
        if self.steps != expected_steps {
            return Err(
                "the MVP supports exactly the streaming 429 followed by successful stream sequence"
                    .into(),
            );
        }
        Ok(())
    }

    fn step_timeout(&self) -> Duration {
        Duration::from_millis(self.step_timeout_ms)
    }

    fn scenario_timeout(&self) -> Duration {
        Duration::from_millis(self.scenario_timeout_ms)
    }

    fn diagnostic(&self, step: &str, detail: impl std::fmt::Display) -> String {
        format!(
            "replay scenario={} schema={} seed={} step={step}: {detail}\nreproduce: \
             just test-inference-replay scenario={}",
            self.scenario_id.label(),
            self.schema_version,
            self.seed,
            self.scenario_id.label()
        )
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Debug)]
struct ProviderReport {
    requests: usize,
    connections: usize,
}

struct ProviderGuard {
    address: SocketAddr,
    task: Option<JoinHandle<Result<ProviderReport, String>>>,
}

impl ProviderGuard {
    async fn start(steps: Vec<Step>) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| format!("failed to bind local replay provider: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("failed to inspect local replay provider: {error}"))?;
        let task = tokio::spawn(run_provider(listener, steps));
        Ok(Self {
            address,
            task: Some(task),
        })
    }

    async fn finish(&mut self, deadline: Duration) -> Result<ProviderReport, String> {
        let task = self
            .task
            .take()
            .ok_or_else(|| "local replay provider was already joined".to_string())?;
        tokio::time::timeout(deadline, task)
            .await
            .map_err(|_| "local replay provider did not finish before its deadline".to_string())?
            .map_err(|error| format!("local replay provider task failed: {error}"))?
    }
}

impl Drop for ProviderGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(crate) async fn run(source: &str) -> Result<(), String> {
    let scenario = Scenario::parse(source)?;
    let deadline = scenario.scenario_timeout();
    tokio::time::timeout(deadline, run_scenario(&scenario))
        .await
        .map_err(|_| scenario.diagnostic("scenario", "hard deadline exceeded"))?
}

async fn run_scenario(scenario: &Scenario) -> Result<(), String> {
    let mut provider = ProviderGuard::start(scenario.steps.clone()).await?;
    let provider_url = format!("http://{}", provider.address);
    let temporary = tempfile::tempdir()
        .map_err(|error| scenario.diagnostic("setup", format!("tempdir failed: {error}")))?;
    let config_path = temporary.path().join("config.toml");
    std::fs::write(&config_path, "")
        .map_err(|error| scenario.diagnostic("setup", format!("config write failed: {error}")))?;
    let stderr = std::fs::File::create(temporary.path().join("gateway.log"))
        .map_err(|error| scenario.diagnostic("setup", format!("log create failed: {error}")))?;
    let relay_address = unused_address().map_err(|error| {
        scenario.diagnostic("setup", format!("port reservation failed: {error}"))
    })?;
    let relay_url = format!("http://{relay_address}");
    let child = Command::new(gateway_bin())
        .arg("--config")
        .arg(&config_path)
        .arg("--bind")
        .arg(relay_address.to_string())
        .arg("--anthropic-base-url")
        .arg(&provider_url)
        .env_remove("NEMO_RELAY_ANTHROPIC_AUTH_HEADER")
        .env_remove("NEMO_RELAY_MAX_HOOK_PAYLOAD_BYTES")
        .env_remove("NEMO_RELAY_MAX_PASSTHROUGH_BODY_BYTES")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| {
            scenario.diagnostic("setup", format!("failed to launch nemo-relay: {error}"))
        })?;
    let mut relay = ChildGuard(child);

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(1)
        .build()
        .map_err(|error| scenario.diagnostic("setup", format!("client build failed: {error}")))?;
    wait_for_gateway(scenario, &client, &relay_url, &mut relay.0).await?;

    let mut failures = Vec::new();
    for (index, step) in scenario.steps.iter().enumerate() {
        if let Err(error) = execute_step(scenario, &client, &relay_url, index, *step).await {
            failures.push(error);
        }
    }

    match provider.finish(scenario.step_timeout()).await {
        Ok(report) if report.requests == scenario.steps.len() && report.connections == 1 => {}
        Ok(report) => failures.push(scenario.diagnostic(
            "provider",
            format!(
                "expected {} requests on one upstream connection, observed {} requests on {} \
                 connections",
                scenario.steps.len(),
                report.requests,
                report.connections
            ),
        )),
        Err(error) => failures.push(scenario.diagnostic("provider", error)),
    }
    if !failures.is_empty() {
        return Err(failures.join("\n"));
    }
    Ok(())
}

fn unused_address() -> Result<SocketAddr, std::io::Error> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

async fn wait_for_gateway(
    scenario: &Scenario,
    client: &reqwest::Client,
    relay_url: &str,
    child: &mut Child,
) -> Result<(), String> {
    let readiness = async {
        loop {
            if let Some(status) = child.try_wait().map_err(|error| {
                scenario.diagnostic("readiness", format!("process check failed: {error}"))
            })? {
                return Err(
                    scenario.diagnostic("readiness", format!("nemo-relay exited with {status}"))
                );
            }
            if client
                .get(format!("{relay_url}/healthz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    tokio::time::timeout(scenario.step_timeout(), readiness)
        .await
        .map_err(|_| scenario.diagnostic("readiness", "gateway readiness deadline exceeded"))?
}

async fn execute_step(
    scenario: &Scenario,
    client: &reqwest::Client,
    relay_url: &str,
    index: usize,
    step: Step,
) -> Result<(), String> {
    let step_label = format!("{}", index + 1);
    let request = client
        .post(format!("{relay_url}/v1/messages"))
        .header("x-api-key", "synthetic-key")
        .header("anthropic-version", "2023-06-01")
        .header("x-nemo-relay-session-id", "synthetic-replay-session")
        .json(&synthetic_request(scenario.seed, index));
    let response = tokio::time::timeout(scenario.step_timeout(), request.send())
        .await
        .map_err(|_| scenario.diagnostic(&step_label, "request deadline exceeded"))?
        .map_err(|error| {
            scenario.diagnostic(&step_label, format!("request transport failed: {error}"))
        })?;

    match step.expected {
        ExpectedOutcome::RateLimitPassthrough => {
            if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(scenario.diagnostic(
                    &step_label,
                    format!("expected HTTP 429, observed HTTP {}", response.status()),
                ));
            }
            if response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                != Some("application/json")
            {
                return Err(scenario.diagnostic(
                    &step_label,
                    "expected the provider Content-Type header to be preserved",
                ));
            }
            if response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                != Some("7")
            {
                return Err(scenario.diagnostic(
                    &step_label,
                    "expected the safe Retry-After header to be preserved",
                ));
            }
            let body = tokio::time::timeout(scenario.step_timeout(), response.text())
                .await
                .map_err(|_| scenario.diagnostic(&step_label, "error body deadline exceeded"))?
                .map_err(|error| {
                    scenario.diagnostic(&step_label, format!("error body read failed: {error}"))
                })?;
            if body != RATE_LIMIT_BODY {
                return Err(scenario.diagnostic(
                    &step_label,
                    format!(
                        "expected preserved synthetic error body ({} bytes), observed {} bytes",
                        RATE_LIMIT_BODY.len(),
                        body.len()
                    ),
                ));
            }
        }
        ExpectedOutcome::StreamSuccess => {
            if !response.status().is_success() {
                return Err(scenario.diagnostic(
                    &step_label,
                    format!(
                        "expected successful stream, observed HTTP {}",
                        response.status()
                    ),
                ));
            }
            if response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                != Some("text/event-stream")
            {
                return Err(scenario.diagnostic(
                    &step_label,
                    "expected the successful response to remain an SSE stream",
                ));
            }
            let body = tokio::time::timeout(scenario.step_timeout(), response.text())
                .await
                .map_err(|_| scenario.diagnostic(&step_label, "stream body deadline exceeded"))?
                .map_err(|error| {
                    scenario.diagnostic(&step_label, format!("stream body read failed: {error}"))
                })?;
            if !body.contains("event: message_start")
                || !body.contains("synthetic-output")
                || !body.contains("event: message_stop")
            {
                return Err(scenario.diagnostic(
                    &step_label,
                    "successful response did not contain the expected synthetic SSE events",
                ));
            }
        }
    }
    Ok(())
}

fn synthetic_request(seed: u64, index: usize) -> Value {
    json!({
        "model": "synthetic-model",
        "max_tokens": 8,
        "stream": true,
        "messages": [{
            "role": "user",
            "content": format!("synthetic-input-{seed}-{index}")
        }]
    })
}

async fn run_provider(listener: TcpListener, steps: Vec<Step>) -> Result<ProviderReport, String> {
    let (stream, _) = listener
        .accept()
        .await
        .map_err(|error| format!("local provider accept failed: {error}"))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("local provider TCP setup failed: {error}"))?;
    let (read, mut write) = tokio::io::split(stream);
    let mut read = BufReader::new(read);

    for (index, step) in steps.iter().enumerate() {
        let request = if index == 0 {
            read_request(&mut read).await?
        } else {
            tokio::select! {
                request = read_request(&mut read) => request?,
                accepted = listener.accept() => {
                    accepted.map_err(|error| format!("local provider accept failed: {error}"))?;
                    return Err(
                        "relay opened a second upstream connection instead of reusing the first"
                            .into(),
                    );
                }
            }
        };
        validate_request(request)?;
        write_response(&mut write, step.provider_response).await?;
    }

    Ok(ProviderReport {
        requests: steps.len(),
        connections: 1,
    })
}

struct RawRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

async fn read_request(read: &mut BufReader<ReadHalf<TcpStream>>) -> Result<RawRequest, String> {
    let mut line = String::new();
    let mut header_bytes = read
        .read_line(&mut line)
        .await
        .map_err(|error| format!("local provider request-line read failed: {error}"))?;
    if header_bytes == 0 {
        return Err("relay closed the upstream connection before the next request".into());
    }
    if header_bytes > MAX_HEADER_BYTES {
        return Err("upstream request headers exceeded the replay limit".into());
    }
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "upstream request line had no method".to_string())?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| "upstream request line had no path".to_string())?
        .to_string();
    let mut content_length = None;

    loop {
        line.clear();
        let bytes = read
            .read_line(&mut line)
            .await
            .map_err(|error| format!("local provider header read failed: {error}"))?;
        if bytes == 0 {
            return Err("relay closed the upstream connection during headers".into());
        }
        header_bytes += bytes;
        if header_bytes > MAX_HEADER_BYTES {
            return Err("upstream request headers exceeded the replay limit".into());
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "upstream request contained a malformed header".to_string())?;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| "upstream request had an invalid Content-Length".to_string())?,
            );
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("chunked upstream requests are outside the MVP replay schema".into());
        }
    }

    let content_length = content_length.unwrap_or(0);
    if content_length > MAX_REQUEST_BYTES {
        return Err("upstream request body exceeded the replay limit".into());
    }
    let mut body = vec![0; content_length];
    read.read_exact(&mut body)
        .await
        .map_err(|error| format!("local provider body read failed: {error}"))?;
    Ok(RawRequest { method, path, body })
}

fn validate_request(request: RawRequest) -> Result<(), String> {
    if request.method != "POST" || request.path != "/v1/messages" {
        return Err(format!(
            "expected POST /v1/messages, observed {} {}",
            request.method, request.path
        ));
    }
    let body = serde_json::from_slice::<Value>(&request.body)
        .map_err(|_| "upstream request body was not valid JSON".to_string())?;
    if body.get("stream").and_then(Value::as_bool) != Some(true) {
        return Err("upstream request did not preserve stream=true".into());
    }
    if body.get("model").and_then(Value::as_str) != Some("synthetic-model") {
        return Err("upstream request did not preserve the synthetic model".into());
    }
    Ok(())
}

async fn write_response(
    write: &mut WriteHalf<TcpStream>,
    response: ProviderResponse,
) -> Result<(), String> {
    let (status, content_type, body, connection, extra_headers) = match response {
        ProviderResponse::RateLimitJsonKeepAlive => (
            "429 Too Many Requests",
            "application/json",
            RATE_LIMIT_BODY,
            "keep-alive",
            "Retry-After: 7\r\n",
        ),
        ProviderResponse::AnthropicSseSuccessClose => {
            ("200 OK", "text/event-stream", SUCCESS_BODY, "close", "")
        }
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: \
         {}\r\nConnection: {connection}\r\n{extra_headers}\r\n",
        body.len()
    );
    write
        .write_all(head.as_bytes())
        .await
        .map_err(|error| format!("local provider response-header write failed: {error}"))?;
    write
        .write_all(body.as_bytes())
        .await
        .map_err(|error| format!("local provider response-body write failed: {error}"))?;
    write
        .flush()
        .await
        .map_err(|error| format!("local provider response flush failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str =
        include_str!("../fixtures/inference-replay/v1/streaming-429-followup.json");

    #[test]
    fn fixture_is_strict_and_valid() {
        Scenario::parse(FIXTURE).unwrap();

        let mut value = serde_json::from_str::<Value>(FIXTURE).unwrap();
        value["copied_source_metadata"] = Value::String("must not survive".into());
        let error = Scenario::parse(&value.to_string()).unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn fixture_rejects_unapproved_string_values() {
        let mut value = serde_json::from_str::<Value>(FIXTURE).unwrap();
        value["steps"][0]["provider_response"] = Value::String("source_response".into());
        let error = Scenario::parse(&value.to_string()).unwrap_err();
        assert!(error.contains("unknown variant"), "{error}");
    }
}
