use std::time::Duration;

use serde::Deserialize;

use crate::{
    pipelining::{BootstrapResult, SinkProvider, StageReceiver},
    sinks::ErrorPolicy,
    utils::{retry, WithUtils},
};

use super::run::writer_loop;

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    pub topic: String,
    pub credentials: String,
    pub error_policy: Option<ErrorPolicy>,
    pub retry_policy: Option<retry::Policy>,
}

const DEFAULT_MAX_RETRIES: u32 = 20;
const DEFAULT_BACKOFF_DELAY: u64 = 5_000;

impl SinkProvider for WithUtils<Config> {
    fn bootstrap(&self, input: StageReceiver) -> BootstrapResult {
        let credentials = self.inner.credentials.to_owned();
        let topic_name = self.inner.topic.to_owned();

        let error_policy = self
            .inner
            .error_policy
            .as_ref()
            .cloned()
            .unwrap_or(ErrorPolicy::Exit);

        let retry_policy = self.inner.retry_policy.unwrap_or(retry::Policy {
            max_retries: DEFAULT_MAX_RETRIES,
            backoff_unit: Duration::from_millis(DEFAULT_BACKOFF_DELAY),
            backoff_factor: 2,
            max_backoff: Duration::from_millis(DEFAULT_BACKOFF_DELAY * 20),
        });

        let utils = self.utils.clone();

        let handle = std::thread::spawn(move || {
            writer_loop(
                input,
                credentials,
                topic_name,
                &error_policy,
                &retry_policy,
                utils,
            )
            .expect("writer loop failed");
        });

        Ok(handle)
    }
}
