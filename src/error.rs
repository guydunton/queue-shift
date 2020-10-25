use rusoto_sqs::Message;

pub enum Error {
    ParameterParseFailed,
    MessagePullFailed,
    NothingAfterFilter,
    FailedToPushMessages,
    FailedToDeleteMessages(Vec<Message>),
}
