// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Black-box inference replay coverage for realistic multi-request gateway behavior.

mod replay;

const STREAMING_429_FOLLOWUP: &str =
    include_str!("fixtures/inference-replay/v1/streaming-429-followup.json");

#[tokio::test(flavor = "multi_thread")]
async fn streaming_429_followup() {
    replay::run(STREAMING_429_FOLLOWUP)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}
