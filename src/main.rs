mod cli;
mod error;
mod queue;

use cli::{get_cli_params, Parameters};
use error::Error;
use futures::future::TryFutureExt;
use queue::{delete_messages_from_queue, pull_messages, push_messages, reset_timeout};
use rusoto_core::{credential::ProfileProvider, HttpClient};
use rusoto_sqs::{Message, SqsClient};

fn do_attributes_match_filter(filter: &(String, String), message: &Message) -> bool {
    // Find all the message attributes matching the filter
    let found_match = message
        .message_attributes
        .as_ref()
        .and_then(|attributes| attributes.get(&filter.0))
        .filter(|val| val.string_value == Some(filter.1.clone()));

    found_match.is_some()
}

fn filter_messages(
    filter: &(String, String),
    messages: Vec<Message>,
) -> (Vec<Message>, Vec<Message>) {
    messages
        .into_iter()
        .partition(|msg| do_attributes_match_filter(filter, msg))
}

fn maybe_filter_messages(
    filter: &Option<(String, String)>,
    messages: Vec<Message>,
) -> (Vec<Message>, Vec<Message>) {
    match filter {
        Some(filter) => filter_messages(filter, messages),
        None => (messages, vec![]),
    }
}

async fn process_messages(
    params: &Parameters,
    sqs: &SqsClient,
    rejected_messages: &mut Vec<Message>,
) -> Result<(), Error> {
    loop {
        let messages = pull_messages(sqs, &params.source).await?;

        // If we didn't get any messages then stop
        if messages.is_empty() {
            return Ok(());
        }

        let (filtered, mut rejected) = maybe_filter_messages(&params.filter, messages);

        // Store rejected messages so we can reduce timeout at the end
        rejected_messages.append(&mut rejected);

        // If we don't have any messages then return error. Do this after recording filtered messages
        if filtered.is_empty() {
            return Err(Error::NothingAfterFilter);
        }

        let pushed_messages = push_messages(sqs, &params.destination, filtered).await?;
        delete_messages_from_queue(sqs, &params.source, pushed_messages).await?;
    }
}

async fn start(params: Parameters) -> Result<(), Error> {
    let home_dir = dirs::home_dir().expect("Could not find home directory");

    let profile_provider = ProfileProvider::with_configuration(
        home_dir.join(".aws/credentials"),
        params.profile.clone(),
    );

    let sqs = SqsClient::new_with(
        HttpClient::new().expect("Failed to create request dispatcher"),
        profile_provider,
        params.region.clone(),
    );

    let mut rejected_messages = vec![];

    let result = process_messages(&params, &sqs, &mut rejected_messages).await;

    // Reset the timeout on the rejected messages
    reset_timeout(&sqs, &params.source, rejected_messages).await?;

    result
}

#[tokio::main]
async fn main() {
    let result: Result<(), Error> = async { get_cli_params() }.and_then(start).await;

    if let Err(err) = result {
        match err {
            Error::ParameterParseFailed => {
                println!("Error: Failed to parse parameters");
                std::process::exit(1);
            }
            Error::MessagePullFailed => {
                println!("Warning: Failed to retrieve messages from source queue. There might not have been any messages on the queue");
            }
            Error::NothingAfterFilter => {
                println!("Warning: No messages matched the filter");
            }
            Error::FailedToPushMessages => {
                println!("Error: Failed to push messages to destination queue");
                std::process::exit(1);
            }
            Error::FailedToDeleteMessages(_) => {
                println!(
                    "Error: Unable to delete messages. Messages may be present both source and destination queues"
                );
                std::process::exit(1);
            }
            Error::FailedToChangeMessageVisibility => {
                println!("Warning: Failed to change visibility of some unfiltered messages. They will not be visible on source queue for 10 minutes")
            }
        };
    }
}
