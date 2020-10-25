# Queue Shift

Move messages from one AWS SQS queue to another. Allows the user to filter based on message attributes

## Usage

The following command moves all messages with a `Type` attribute equal to `greeting`:

```bash
queue-shift \
--source https://sqs.us-east-1.amazonaws.com/0123456789/Queue1 \
--dest https://sqs.us-east-1.amazonaws.com/0123456789/Queue2 \
--filter Type=greeting
```

Filters are optional. The following command moves all messages to another queue:

```bash
queue-shift \
--source https://sqs.us-east-1.amazonaws.com/0123456789/Queue1 \
--dest https://sqs.us-east-1.amazonaws.com/0123456789/Queue2
```

Your AWS profile and region can be set using either environment variables or parameters:

```bash
AWS_PROFILE=basic AWS_DEFAULT_REGION=us-east-1 queue-shift ...

# Is equivalent to
queue-shift --profile basic --region us-east-1
```

## To build

### Prerequisites

- AWS profile setup
- Rust installed. Instructions [here](https://www.rust-lang.org/tools/install).

### How to build

Simply build using cargo:

```bash
cargo build --release
```

## How to run

After building the code run the code from the build directory

```bash
cd target/release
./queue-shift --help
```
