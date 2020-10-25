use rusoto_sqs::{
    DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, Message, ReceiveMessageRequest,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs,
};

use crate::error::Error;

pub async fn pull_all_messages<T: Sqs>(sqs: &T, queue_url: &str) -> Result<Vec<Message>, Error> {
    let mut messages = vec![];
    loop {
        let response = sqs
            .receive_message(ReceiveMessageRequest {
                attribute_names: Some(vec!["All".to_owned()]),
                max_number_of_messages: Some(10),
                message_attribute_names: Some(vec!["All".to_owned()]),
                queue_url: queue_url.to_owned(),
                receive_request_attempt_id: None,
                visibility_timeout: Some(2),
                wait_time_seconds: Some(1),
            })
            .await;

        let maybe_messages = response
            .ok()
            .and_then(|val| val.messages)
            .filter(|messages| !messages.is_empty());

        match maybe_messages {
            Some(mut new_messages) => {
                messages.append(&mut new_messages);
            }
            None => {
                break;
            }
        }
    }

    if messages.is_empty() {
        Err(Error::MessagePullFailed)
    } else {
        Ok(messages)
    }
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
