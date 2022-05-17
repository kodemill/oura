use std::{sync::Arc, time::Duration};

use reqwest::blocking::Client;
use serde::Serialize;

use crate::{
    model::Event,
    pipelining::StageReceiver,
    sinks::ErrorPolicy,
    utils::{retry, Utils},
    Error,
};

#[derive(Serialize)]
struct RequestBody {
    #[serde(flatten)]
    event: Event,
    variant: String,
    timestamp: Option<u64>,
}

impl From<Event> for RequestBody {
    fn from(event: Event) -> Self {
        let timestamp = event.context.timestamp.map(|x| x * 1000);
        let variant = event.data.to_string();

        RequestBody {
            event,
            timestamp,
            variant,
        }
    }
}

fn execute_fallible_request(client: &Client, url: &str, body: &RequestBody) -> Result<(), Error> {
    let request = client.post(url).json(body).build()?;

    client
        .execute(request)
        .and_then(|res| res.error_for_status())?;

    Ok(())
}

pub(crate) fn request_loop(
    input: StageReceiver,
    client: &Client,
    url: &str,
    error_policy: &ErrorPolicy,
    max_retries: u32,
    backoff_delay: Duration,
    utils: Arc<Utils>,
) -> Result<(), Error> {
    for event in input.iter() {
        // notify progress to the pipeline
        utils.track_sink_progress(&event);

        let body = RequestBody::from(event);

        let result = retry::retry_operation(
            || execute_fallible_request(client, url, &body),
            &retry::Policy {
                max_retries,
                backoff_unit: backoff_delay,
                backoff_factor: 2,
                max_backoff: backoff_delay * 2,
            },
        );

        match result {
            Ok(()) => (),
            Err(err) => match error_policy {
                ErrorPolicy::Exit => return Err(err),
                ErrorPolicy::Continue => {
                    log::warn!("failed to send webhook request: {:?}", err);
                }
            },
        }
    }

    Ok(())
}
