use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::observability::NosMetrics;

/// Human: Per-bucket HTTP callback URLs for object lifecycle notifications.
/// Agent: Parsed from NOS_WEBHOOKS_JSON; empty map disables dispatch.
#[derive(Clone, Default)]
pub struct WebhookConfig(pub HashMap<String, Vec<String>>);

impl WebhookConfig {
    pub fn from_json(raw: &str) -> anyhow::Result<Self> {
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }
        let map: HashMap<String, Vec<String>> =
            serde_json::from_str(raw).context("NOS_WEBHOOKS_JSON must be a JSON object")?;
        Ok(Self(map))
    }

    pub fn urls_for_bucket(&self, bucket: &str) -> Vec<String> {
        self.0
            .get(bucket)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|u| !u.trim().is_empty())
            .collect()
    }
}

#[derive(Debug, Serialize)]
struct WebhookPayload<'a> {
    event: &'a str,
    bucket: &'a str,
    key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<&'a str>,
}

/// Human: Fire-and-forget webhook delivery after successful object mutations.
/// Agent: tokio::spawn per URL; failures increment metrics only; never blocks PUT/DELETE.
#[derive(Clone)]
pub struct WebhookDispatcher {
    client: reqwest::Client,
    config: WebhookConfig,
    metrics: Arc<NosMetrics>,
}

impl WebhookDispatcher {
    pub fn new(config: WebhookConfig, metrics: Arc<NosMetrics>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            config,
            metrics,
        }
    }

    pub fn dispatch_put(&self, bucket: &str, key: &str, size: i64, etag: Option<&str>) {
        self.dispatch(
            "object.put",
            bucket,
            key,
            Some(size),
            etag,
        );
    }

    pub fn dispatch_delete(&self, bucket: &str, key: &str) {
        self.dispatch("object.delete", bucket, key, None, None);
    }

    fn dispatch(
        &self,
        event: &str,
        bucket: &str,
        key: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) {
        let urls = self.config.urls_for_bucket(bucket);
        if urls.is_empty() {
            return;
        }
        let payload = WebhookPayload {
            event,
            bucket,
            key,
            size,
            etag,
        };
        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => return,
        };
        for url in urls {
            let client = self.client.clone();
            let metrics = self.metrics.clone();
            let url = url.clone();
            let body = body.clone();
            tokio::spawn(async move {
                match client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {}
                    Ok(resp) => {
                        tracing::warn!(
                            url = %url,
                            status = %resp.status(),
                            "webhook delivery returned non-success"
                        );
                        metrics.inc_webhook_errors();
                    }
                    Err(e) => {
                        tracing::warn!(url = %url, error = %e, "webhook delivery failed");
                        metrics.inc_webhook_errors();
                    }
                }
            });
        }
    }
}

use anyhow::Context;
