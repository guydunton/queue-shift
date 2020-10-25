mod cli;
mod error;
mod queue;

use cli::{get_cli_params, Parameters};
use error::Error;
use futures::future::TryFutureExt;
use queue::{delete_messages_from_queue, pull_all_messages, push_messages};
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
) -> Result<Vec<Message>, Error> {
    let filtered: Vec<Message> = messages
        .into_iter()
        .filter(|msg| do_attributes_match_filter(filter, msg))
        .collect();

    if !filtered.is_empty() {
        Ok(filtered)
    } else {
        Err(Error::NothingAfterFilter)
    }
}

fn maybe_filter_messages(
    filter: &Option<(String, String)>,
    messages: Vec<Message>,
) -> Result<Vec<Message>, Error> {
    match filter {
        Some(filter) => filter_messages(filter, messages),
        None => Ok(messages),
    }
}

async fn move_messages(params: Parameters) -> Result<(), Error> {
    let home_dir = dirs::home_dir().expect("Could not find home directory");

    let profile_provider =
        ProfileProvider::with_configuration(home_dir.join(".aws/credentials"), params.profile);

    let sqs = SqsClient::new_with(
        HttpClient::new().expect("Failed to create request dispatcher"),
        profile_provider,
        params.region,
    );

    let mut messages = pull_all_messages(&sqs, &params.source).await?;
    messages = maybe_filter_messages(&params.filter, messages)?;
    messages = push_messages(&sqs, &params.destination, messages).await?;
    delete_messages_from_queue(&sqs, &params.source, messages).await
}

#[tokio::main]
async fn main() {
    let result: Result<(), Error> = async { get_cli_params() }.and_then(move_messages).await;

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
        };
    }
}
