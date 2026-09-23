use crate::{NRTMMessage, NRTMParser, NRTMV2Parser, NRTMV3Parser, ParseError};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use futures_util::{TryStream, TryStreamExt};
use re_delimiter_codec::{REDelimiterCodec, REDelimiterCodecError};
use regex::bytes::Regex;
use tokio::io::AsyncRead;
use tokio_util::codec::FramedRead;

#[derive(Clone)]
pub(crate) struct NRTMDec {
    parser: fn(&str) -> Result<NRTMMessage, ParseError>,
    max_chunk_len: usize,
}

#[derive(Debug)]
pub enum NRTMStreamError {
    REDelimiterCodec(REDelimiterCodecError),
    Parser(ParseError),
}

fn new_nrtm_preparser(with_max_chunk_len: usize) -> REDelimiterCodec {
    // ok to call unwrap here, we know this will not fail
    REDelimiterCodec::new_with_max_length(
        // will slice at each end of an NRTM object.
        // NRTM delimiters are double new lines.
        //
        // 1st part of the regex: \n[^%].*
        //
        // we add a negative match in order to not match comment lines
        // which are part of the prelude. sometimes NRTM preludes contain
        // double newlines.
        //
        //
        // 2nd part of the regex: \n[^AD][^DE][^DL].*\n\n
        //
        // we add a negative match in front of the delimiter,
        // which DOES NOT match the start of an ADD/DEL operation (v2/v3).
        // That operation will always be after the start of a newline (cannot
        // have a line continuation, as opposed to RPSL).
        // This is akin to negative lookbehind but ofc not as precise...
        // but it's good enough for us. I am not using alternate patterns
        //
        // This has the effect of excluding this exact double newline separator
        // and thus, we have the full NRTM message, without odd/even pairs.
        // It will also not match RPSL line continuations, since the two \n
        // are not right next to each other (at least one character is between them)
        //
        // remarks: RPSL objects have at least 1 class attr and 1 attr from the class,
        // so having two entire lines on the "back" part of the regex is okay. these will
        // match the RPSL object partially or entirely.
        //
        // EXAMPLES:
        //
        // \nDEL 65934907\n\n                                      <- this does not match
        //
        // \nmulti-line: attribute\n multi-line: goes on\n         <- this does not match
        //
        // \nremarks:        ****************************\n\n      <- this *does* match. end of RPSL
        //
        // which is what we want, since we need to cut at the end of the RPSL object.
        Regex::new(r"(?R)\n[^%].*\n[^AD][^DE][^DL].*\n\n").unwrap(),
        with_max_chunk_len,
    )
}

impl NRTMDec {
    pub(crate) fn new_v2(max_chunk_len: usize) -> Self {
        NRTMDec {
            parser: NRTMV2Parser::try_parse,
            max_chunk_len,
        }
    }

    pub(crate) fn new_v3(max_chunk_len: usize) -> Self {
        NRTMDec {
            parser: NRTMV3Parser::try_parse,
            max_chunk_len,
        }
    }
    pub(crate) fn get_stream<T: AsyncRead>(
        &mut self,
        reader: T,
    ) -> impl TryStream<Ok = NRTMMessage, Error = NRTMStreamError> {
        let framed_reader = FramedRead::new(reader, new_nrtm_preparser(self.max_chunk_len));
        let parser = self.parser;

        framed_reader
            .and_then(
                // charset guesstimation
                |chunk| {
                    // for each chunk we need a new instance of detector,
                    // as each chunk potentially has a different charset
                    let mut encoding_detector = EncodingDetector::new(Iso2022JpDetection::Allow);
                    // re ISO 2022 JP, we don't care about preventing XSS, this is the job of either
                    // the data source or the client. we're only a middle layer and so
                    // should not meddle with the data.

                    // feed all the chunk data in the encoding detector
                    encoding_detector.feed(chunk.as_ref(), true);

                    // there is no encoding spec for NRTMv2 and NRTMv3, so allowing UTF-8 as a
                    // detection target is fine
                    let encoding = encoding_detector.guess(None, Utf8Detection::Allow);

                    // result is String, in most cases no copy will happen. copy will only happen
                    // for replacement characters insertion
                    let (decoded, _, _) = encoding.decode(chunk.as_ref());

                    futures_util::future::ready(Ok(decoded.into_owned()))
                },
            )
            .map_err(NRTMStreamError::REDelimiterCodec) // use same error type for encapsulation
            .and_then(move |cow_str| {
                let r = match parser(cow_str.as_ref()) {
                    Ok(message) => Ok(message),
                    Err(e) => Err(NRTMStreamError::Parser(e)), // client should recover
                };
                futures_util::future::ready(r)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NRTMMessage, OpType, ParseError};
    use futures_util::TryStreamExt;
    use std::assert_matches;
    use std::io::{Error as IOError, ErrorKind};
    use tokio::fs::File;
    use tokio_test::io::Builder;

    const TEST_MAX_CHUNK_LEN: usize = 131072; // 128K

    #[tokio::test]
    async fn v3_charset_guesstimation_ok() {
        let nrtmv3_sample = File::open("./src/tests/nrtmv3_ripe_mixed_encoding_sample.txt")
            .await
            .unwrap();
        let mut decoder = NRTMDec::new_v3(TEST_MAX_CHUNK_LEN);
        let mut stream = decoder.get_stream(nrtmv3_sample);
        let mut linear_increase_id_counter = 65934900;

        loop {
            match stream.try_next().await {
                Ok(Some(NRTMMessage {
                    update: OpType::V3(_, ctr),
                    rpsl: _,
                    span: _,
                })) => {
                    assert!(linear_increase_id_counter < ctr); // check strictly increasing
                    linear_increase_id_counter = ctr;
                }
                Ok(Some(NRTMMessage {
                    update: OpType::V2(_),
                    rpsl: _,
                    span: _,
                })) => {} // ignore
                Err(e) => panic!("got error {:?}", e),
                Ok(None) => break, // end of stream
            }
        }

        assert_eq!(linear_increase_id_counter, 65934960); // last object id
    }

    #[tokio::test]
    async fn v3_parser_error_signalled() {
        let mut decoder = NRTMDec::new_v3(TEST_MAX_CHUNK_LEN);
        let mut reader = decoder.get_stream(
            &b"\
ADD 324876

object: property
\\xxt*some-more: properties
end-of: object

"[..],
        );
        assert_matches!(
            reader.try_next().await,
            Err(NRTMStreamError::Parser(ParseError::Parser(_)))
        );
    }

    #[tokio::test]
    async fn v3_malformed_serial_signalled() {
        let mut decoder = NRTMDec::new_v3(TEST_MAX_CHUNK_LEN);
        let mut reader = decoder.get_stream(
            &b"\
# should not fit into u64
ADD 99999999999999999999

start-field:    yes
netname:        TRANSPORT-NET

"[..],
        );
        assert_matches!(
            reader.try_next().await,
            Err(NRTMStreamError::Parser(ParseError::MalformedSerial(_, _)))
        );
    }

    #[tokio::test]
    async fn v3_no_chunks_signalled() {
        let mut decoder = NRTMDec::new_v3(TEST_MAX_CHUNK_LEN);
        let mut reader = decoder.get_stream(
            &b"\
% The RIPE Database is subject to Terms and Conditions.
% See https://docs.db.ripe.net/terms-conditions.html

% lou: I am commenting that out, as in rx only stream
% commands will not appear
% -kg RIPE:3:65776764-LAST
%START Version: 3 RIPE 65776764-65776784
# comment
"[..],
        );

        assert_matches!(
            reader.try_next().await,
            Err(NRTMStreamError::REDelimiterCodec(
                REDelimiterCodecError::Io(_)
            ))
        );
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
        let mut decoder = NRTMDec::new_v3(TEST_MAX_CHUNK_LEN);
        let mut reader = decoder.get_stream(ioerroring_sample);

        reader.try_next().await.unwrap().unwrap(); // chuck first object
        assert_matches!(
            reader.try_next().await,
            Err(NRTMStreamError::REDelimiterCodec(
                REDelimiterCodecError::Io(_)
            ))
        );
    }
}
