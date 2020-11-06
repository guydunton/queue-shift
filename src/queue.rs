use rusoto_sqs::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, Message, ReceiveMessageRequest,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs,
};

use crate::error::Error;

pub async fn pull_messages<T: Sqs>(sqs: &T, queue_url: &str) -> Result<Vec<Message>, Error> {
    let response = sqs
        .receive_message(ReceiveMessageRequest {
            attribute_names: Some(vec!["All".to_owned()]),
            max_number_of_messages: Some(10),
            message_attribute_names: Some(vec!["All".to_owned()]),
            queue_url: queue_url.to_owned(),
            receive_request_attempt_id: None,
            visibility_timeout: Some(60 * 10),
            wait_time_seconds: Some(1),
        })
        .await;

    let maybe_messages = response.map(|val| val.messages.unwrap_or(vec![]));

    maybe_messages.map_err(|_| Error::MessagePullFailed)
}

pub async fn push_messages<T: Sqs>(
    sqs: &T,
    dest: &str,
    messages: Vec<Message>,
) -> Result<Vec<Message>, Error> {
    // for each message send the message to the destination queue

    let send_messages: Vec<SendMessageBatchRequestEntry> = messages
        .iter()
        .map(|message| SendMessageBatchRequestEntry {
            delay_seconds: Some(0),
            id: message.message_id.clone().unwrap(),
            message_attributes: message.message_attributes.clone(),
            message_body: message.body.clone().unwrap(),
            message_deduplication_id: None,
            message_group_id: None,
            message_system_attributes: None,
        })
        .collect();

    let response = sqs
        .send_message_batch(SendMessageBatchRequest {
            entries: send_messages,
            queue_url: dest.to_owned(),
        })
        .await;

    // If there was an error then return the passed in messages
    match response {
        Ok(_) => Ok(messages),
        Err(_) => Err(Error::FailedToPushMessages),
    }
}

pub async fn delete_messages_from_queue<T: Sqs>(
    sqs: &T,
    queue_url: &str,
    messages: Vec<Message>,
) -> Result<(), Error> {
    let delete_messages: Vec<DeleteMessageBatchRequestEntry> = messages
        .iter()
        .map(|message| DeleteMessageBatchRequestEntry {
            id: message.message_id.clone().unwrap(),
            receipt_handle: message.receipt_handle.clone().unwrap(),
        })
        .collect();

    let response = sqs
        .delete_message_batch(DeleteMessageBatchRequest {
            entries: delete_messages,
            queue_url: queue_url.to_owned(),
        })
        .await;

    match response {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::FailedToDeleteMessages(messages)),
    }
}

pub async fn reset_timeout<T: Sqs>(
    sqs: &T,
    queue_url: &str,
    messages: Vec<Message>,
) -> Result<(), Error> {
    // Split into batches and reset the timeout
    let chunks = messages.chunks(10);

    for chunk in chunks {
        sqs.change_message_visibility_batch(ChangeMessageVisibilityBatchRequest {
            entries: chunk
                .iter()
                .map(|msg| ChangeMessageVisibilityBatchRequestEntry {
                    id: msg.message_id.clone().unwrap(),
                    receipt_handle: msg.receipt_handle.clone().unwrap(),
                    visibility_timeout: Some(1),
                })
                .collect(),
            queue_url: queue_url.to_owned(),
        })
        .await
        .map_err(|_| Error::FailedToChangeMessageVisibility)?;
    }

    Ok(())
}
