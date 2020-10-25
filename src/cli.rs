use std::str::FromStr;

use clap::{App, Arg, ArgMatches};
use rusoto_core::Region;

use crate::error::Error;

#[derive(Debug)]
pub struct Parameters {
    pub source: String,
    pub destination: String,
    pub filter: Option<(String, String)>,
    pub region: Region,
    pub profile: String,
}

fn get_filter(value: &str) -> Option<(String, String)> {
    let parts = value.split('=');
    match (parts.clone().nth(0), parts.clone().nth(1)) {
        (Some(attrib), Some(val)) => Some((attrib.to_owned(), val.to_owned())),
        _ => None,
    }
}

impl Parameters {
    fn parse_from_matches(matches: ArgMatches) -> Option<Self> {
        let source = matches.value_of("source").map(String::from)?;
        let destination = matches.value_of("dest").map(String::from)?;
        let profile = matches.value_of("profile").map(String::from)?;
        let filter = matches.value_of("filter").map(get_filter).flatten();
        let region = matches
            .value_of("region")
            .map(|v| Region::from_str(v))?
            .ok()?;

        Some(Parameters {
            source,
            destination,
            filter,
            region,
            profile,
        })
    }
}

pub fn get_cli_params() -> Result<Parameters, Error> {
    let filter_validator = |text: String| {
        let parts: Vec<&str> = text.split('=').collect();
        if parts.len() == 2 {
            Ok(())
        } else {
            Err("Filter must follow the pattern attribute=value".to_owned())
        }
    };

    let matches = App::new("Queue Shift")
        .about("Filter message from an SQS queue onto another queue")
        .arg(
            Arg::with_name("source")
                .long("source")
                .short("s")
                .required(true)
                .takes_value(true)
                .help("The Queue url to pull messages from"),
        )
        .arg(
            Arg::with_name("dest")
                .long("dest")
                .short("d")
                .required(true)
                .takes_value(true)
                .help("The Queue url to send messages to"),
        )
        .arg(
            Arg::with_name("filter")
                .long("filter")
                .short("f")
                .takes_value(true)
                .validator(filter_validator)
                .help("Optional filter for message attributes. Format Attribute=Value")
                .long_help(
                    r#"Filter messages based on an attribute. Filters should be of the
format Attribute=Value. Only messages containing the attribute with the correct 
value will be moved. If not specified then all messages are moved"#,
                ),
        )
        .arg(
            Arg::with_name("profile")
                .long("profile")
                .help("AWS profile to use to connect to the queues")
                .env("AWS_PROFILE")
                .takes_value(true)
                .hide_env_values(true)
                .required(true),
        )
        .arg(
            Arg::with_name("region")
                .long("region")
                .help("AWS region of the queues")
                .env("AWS_DEFAULT_REGION")
                .takes_value(true)
                .hide_env_values(true)
                .required(true),
        )
        .get_matches();

    Parameters::parse_from_matches(matches).ok_or(Error::ParameterParseFailed)
}
