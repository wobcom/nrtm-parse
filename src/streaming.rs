use crate::{NRTMMessage, NRTMParser, NRTMV2Parser, NRTMV3Parser, ParseError};
use tokio_util::bytes::BytesMut;
use tokio_util::codec::Decoder;

const MIN_BUFFER_LEN: usize = 8192;
const MIN_DECODE_LEN: usize = "ADD 1".len();

#[derive(Clone)]
pub struct NRTMDec {
    parser: fn(&str) -> Result<NRTMMessage, ParseError>,
}

impl NRTMDec {
    pub fn new_v2() -> Self {
        NRTMDec {
            parser: NRTMV2Parser::try_parse,
        }
    }

    pub fn new_v3() -> Self {
        NRTMDec {
            parser: NRTMV3Parser::try_parse,
        }
    }
}

impl Decoder for NRTMDec {
    type Item = NRTMMessage;
    type Error = ParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        src.reserve(MIN_BUFFER_LEN);

        // from_utf8 is no-copy
        let str = String::from_utf8(src.to_vec()).map_err(ParseError::NonUTF8Input)?;

        // bail early if buffer is empty, as a parser run on empty and / or undecidable str
        // is a waste of cycles and may yield errors
        if src.is_empty() || src.len() <= MIN_DECODE_LEN {
            return Ok(None);
        }

        match (self.parser)(str.as_str()) {
            Ok(message) => {
                let _message_bytes = src.split_to(message.span.end_b);
                // implicit drop for message_str and message_bytes
                Ok(Some(message))
            }
            // per tokio-util codec documentation,
            // If the bytes look valid, but a frame isn’t fully available yet,
            // then Ok(None) is returned.
            Err(ParseError::Incomplete) => Ok(None),
            Err(ParseError::LeadingGarbage(span)) => {
                // split garbage, flush and return err
                let _garbage_bytes = src.split_to(span.end_b);
                Err(ParseError::LeadingGarbage(span))
            }
            // malformed input, split at malformed, flush and return err
            // the rest will be eaten as leading garbage
            Err(ParseError::Parser(err)) => {
                let _malformed_bytes = match err.location {
                    // boop it! kick it! drop it!
                    pest::error::InputLocation::Pos(u) => src.split_to(u),
                    pest::error::InputLocation::Span((_, end)) => src.split_to(end),
                };
                Err(ParseError::Parser(err))
            }
            // malformed utf-8, flush incriminated section
            Err(ParseError::NonUTF8Input(u8)) => {
                let _utf8err_bytes = &mut src.split_to(u8.utf8_error().valid_up_to());
                Err(ParseError::NonUTF8Input(u8))
            }
            // malformed serial
            Err(ParseError::MalformedSerial(span, e)) => {
                let _garbage_bytes = src.split_to(span.end_b); // ibid
                Err(ParseError::MalformedSerial(span, e))
            }
            Err(ParseError::NoMatch) => Err(ParseError::NoMatch),
            Err(ParseError::IoError(ioe)) => {
                src.clear(); // clear buffer
                Err(ParseError::IoError(ioe))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::streaming::NRTMDec;
    use crate::tests::streaming_fixtures;
    use crate::{OpType, ParseError, Verb};
    use std::assert_matches;
    use std::io::{Error as IOError, ErrorKind};
    use tokio::fs::File;
    use tokio_stream::StreamExt;
    use tokio_test::io::Builder;
    use tokio_util::codec::FramedRead;

    #[tokio::test]
    async fn v3_message_stream_ok() {
        let nrtmv3_sample = File::open("./src/tests/nrtmv3_ripe_sample.txt")
            .await
            .unwrap();
        let decoder = NRTMDec::new_v3();
        let mut reader = FramedRead::new(nrtmv3_sample, decoder);
        let mut linear_id_counter = 65776763;

        while let Some(Ok(res)) = reader.next().await {
            match res.update {
                OpType::V3(Verb::ADD, ctr) => {
                    linear_id_counter += 1;
                    assert_eq!(ctr, linear_id_counter);
                }
                _ => panic!("incorrect update {:?}", res.update),
            }
        }
        assert_eq!(linear_id_counter, 65776785); // last object id
    }

    #[tokio::test]
    async fn v3_parser_error_signalled() {
        let mut reader = streaming_fixtures::v3_reader_from(
            b"\
ADD 324876

object: property
\\xxt*some-more: properties
end-of: object

",
        );
        assert_matches!(reader.next().await, Some(Err(ParseError::Parser(_))));
    }

    #[tokio::test]
    async fn v3_non_utf8_error_signalled() {
        let mut reader = streaming_fixtures::v3_reader_from(
            b"\
ADD 324876

object: property\xF8\xF8
some-more: properties
end-of: object

",
        );
        assert_matches!(reader.next().await, Some(Err(ParseError::NonUTF8Input(_))));
    }

    #[tokio::test]
    async fn v3_malformed_serial_signalled() {
        let mut reader = streaming_fixtures::v3_reader_from(
            b"\
# should not fit into u64
ADD 99999999999999999999

start-field:    yes
netname:        TRANSPORT-NET

",
        );
        assert_matches!(
            reader.next().await,
            Some(Err(ParseError::MalformedSerial(_, _)))
        );
    }

    #[tokio::test]
    async fn v3_no_match_signalled() {
        let mut reader = streaming_fixtures::v3_reader_from(
            b"\
% The RIPE Database is subject to Terms and Conditions.
% See https://docs.db.ripe.net/terms-conditions.html

% lou: I am commenting that out, as in rx only stream
% commands will not appear
% -kg RIPE:3:65776764-LAST
%START Version: 3 RIPE 65776764-65776784
# comment
",
        );

        assert_matches!(reader.next().await, Some(Err(ParseError::IoError(_))));
    }

    #[tokio::test]
    async fn v3_io_error_signalled() {
        let nrtmv3_truncated_message = b"\
ADD 324876

object: property
some-more: properties
end-of: object

ADD 324876

object: property
some-more: properties
end-of: obj
";
        let ioerroring_sample = Builder::new()
            .read(nrtmv3_truncated_message)
            .read_error(IOError::new(ErrorKind::BrokenPipe, "connection closed"))
            .build();
        let decoder = NRTMDec::new_v3();
        let mut reader = FramedRead::new(ioerroring_sample, decoder);

        reader.next().await.unwrap().unwrap(); // chuck first object
        assert_matches!(reader.next().await, Some(Err(ParseError::IoError(_))));
    }
}
