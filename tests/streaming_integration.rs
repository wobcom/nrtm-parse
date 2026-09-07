#[cfg(feature = "async-streaming")]
use {
    nrtm_parser::StreamingNRTMParser,
    nrtm_parser::{NRTMV3Parser, OpType, Verb},
    tokio::fs::File,
    tokio_stream::StreamExt,
};

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn parse_message_stream_example() {
    let nrtmv3_sample = File::open("./src/tests/nrtmv3_ripe_sample.txt")
        .await
        .unwrap();
    let mut parser = NRTMV3Parser::reader_from(nrtmv3_sample);

    loop {
        let optional_result = parser.next().await;

        match optional_result {
            None => {} // end of stream
            Some(result) => match result {
                Ok(nrtm_message) => match nrtm_message.update {
                    OpType::V2(_) => {} // ignore v2
                    OpType::V3(verb, serial) => match verb {
                        Verb::ADD => {
                            println!(
                                "operation {serial}, adding rpsl object {}",
                                nrtm_message.rpsl
                            );
                        }
                        Verb::DEL => {
                            println!(
                                "operation {serial}, deleting rpsl object {}",
                                nrtm_message.rpsl
                            );
                        }
                    },
                },
                Err(error) => panic!(
                    "should have parsed successfully but received error {:?}",
                    error
                ),
            },
        }
    }
}
