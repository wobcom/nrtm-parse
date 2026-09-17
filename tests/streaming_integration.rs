#[cfg(feature = "async-streaming")]
use {
    futures_util::TryStreamExt,
    nrtm_parser::streaming::NRTMStreamError,
    nrtm_parser::{NRTMV3Parser, OpType, ParseError, StreamingNRTMParser, Verb},
    tokio::fs::File,
};

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn parse_message_stream_example() {
    let nrtmv3_sample = File::open("./src/tests/nrtmv3_ripe_mixed_encoding_sample.txt")
        .await
        .unwrap();
    let mut parser = NRTMV3Parser::new();
    let mut stream = parser.stream_from(nrtmv3_sample);

    loop {
        let optional_result = stream.try_next().await;

        match optional_result {
            Err(NRTMStreamError::Parser(ParseError::NoMatch))
            | Err(NRTMStreamError::Parser(ParseError::Incomplete))
            | Err(NRTMStreamError::Parser(ParseError::Parser(_)))
            | Err(NRTMStreamError::Parser(ParseError::MalformedSerial(_, _)))
            | Err(NRTMStreamError::Parser(ParseError::LeadingGarbage(_))) => {
                // do nothing, all of these are retryable errors
                // (we should have at least one incomplete parse error due to prelude / comment
                // being chunked). ideally for each error type you'd want to log it, so that
                // we know some garbage data was in the stream.
            }
            Err(error) => panic!(
                "irrecoverable error during consumption of NRTM stream {:?}",
                error
            ),
            Ok(None) => break, // end of stream
            Ok(Some(message)) => {
                match message.update {
                    OpType::V2(_) => {} // ignore v2
                    OpType::V3(verb, serial) => match verb {
                        Verb::ADD => {
                            println!("operation {serial}, adding rpsl object {}", message.rpsl);
                        }
                        Verb::DEL => {
                            println!("operation {serial}, deleting rpsl object {}", message.rpsl);
                        }
                    },
                }
            }
        }
    }
}
