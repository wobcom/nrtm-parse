#[cfg(test)]
mod tests;

use pest::error::{Error, InputLocation};
use pest::iterators::Pair;
use pest_derive::Parser;
use std::mem::discriminant;

#[cfg(feature = "async-streaming")]
pub mod streaming;
#[cfg(feature = "async-streaming")]
use {
    crate::streaming::{NRTMDec, NRTMStreamError},
    futures_util::TryStream,
    tokio::io::AsyncRead,
};

#[derive(Debug, Parser)]
#[grammar = "./grammar.pest"]
pub(crate) struct PestNRTMParser;

#[derive(Debug)]
pub enum Verb {
    ADD,
    DEL,
}

#[derive(Debug)]
pub enum OpType {
    V2(Verb),
    V3(Verb, u64),
}

// this is just to avoid exposing pest API directly
// to user crates
#[derive(Debug)]
pub struct Span {
    pub start_b: usize,
    pub end_b: usize,
    pub str: String,
}

impl From<pest::Span<'_>> for Span {
    fn from(pest_span: pest::Span) -> Span {
        Span {
            start_b: pest_span.start(),
            end_b: pest_span.end(),
            str: String::from(pest_span.as_str()),
        }
    }
}

#[derive(Debug)]
pub struct NRTMMessage {
    pub update: OpType,
    pub rpsl: String,
    pub span: Span,
}

#[derive(Debug)]
pub enum ParseError {
    NoMatch,
    Incomplete,
    Parser(Error<Rule>),
    MalformedSerial(Span, std::num::ParseIntError),
    LeadingGarbage(Span),
    IoError(std::io::Error),
}
impl From<std::io::Error> for ParseError {
    fn from(io_err: std::io::Error) -> Self {
        ParseError::IoError(io_err)
    }
}

pub trait NRTMParser {
    fn try_parse(str: &str) -> Result<NRTMMessage, ParseError>;
}

#[cfg(feature = "async-streaming")]
pub trait StreamingNRTMParser<T>
where
    T: AsyncRead,
{
    fn stream_from(
        &mut self,
        reader: T,
    ) -> impl TryStream<Ok = NRTMMessage, Error = NRTMStreamError>;
}

#[cfg_attr(not(feature = "async-streaming"), derive(Default))]
pub struct NRTMV3Parser {
    #[cfg(feature = "async-streaming")]
    inner_decoder: NRTMDec,
}

impl NRTMV3Parser {
    #[cfg(feature = "async-streaming")]
    pub fn new(max_chunk_len: usize) -> Self {
        Self {
            inner_decoder: NRTMDec::new_v3(max_chunk_len),
        }
    }
    #[cfg(not(feature = "async-streaming"))]
    pub fn new() -> Self {
        Self {}
    }
}
impl NRTMParser for NRTMV3Parser {
    fn try_parse(str: &str) -> Result<NRTMMessage, ParseError> {
        try_parse_nrtm(Rule::v3_operation, str)
    }
}

#[cfg_attr(not(feature = "async-streaming"), derive(Default))]
pub struct NRTMV2Parser {
    #[cfg(feature = "async-streaming")]
    inner_decoder: NRTMDec,
}

impl NRTMV2Parser {
    #[cfg(feature = "async-streaming")]
    pub fn new(max_chunk_len: usize) -> Self {
        Self {
            inner_decoder: NRTMDec::new_v2(max_chunk_len),
        }
    }
    #[cfg(not(feature = "async-streaming"))]
    pub fn new() -> Self {
        Self {}
    }
}
impl NRTMParser for NRTMV2Parser {
    fn try_parse(str: &str) -> Result<NRTMMessage, ParseError> {
        try_parse_nrtm(Rule::v2_operation, str)
    }
}

#[cfg(feature = "async-streaming")]
impl<T: AsyncRead> StreamingNRTMParser<T> for NRTMV2Parser {
    fn stream_from(
        &mut self,
        reader: T,
    ) -> impl TryStream<Ok = NRTMMessage, Error = NRTMStreamError> {
        self.inner_decoder.get_stream(reader)
    }
}

#[cfg(feature = "async-streaming")]
impl<T: AsyncRead> StreamingNRTMParser<T> for NRTMV3Parser {
    fn stream_from(
        &mut self,
        reader: T,
    ) -> impl TryStream<Ok = NRTMMessage, Error = NRTMStreamError> {
        self.inner_decoder.get_stream(reader)
    }
}

fn try_parse_nrtm(root_type: Rule, str: &str) -> Result<NRTMMessage, ParseError> {
    use pest::Parser;

    let res = PestNRTMParser::parse(root_type, str);

    match res {
        Err(e) => match e {
            Error {
                variant:
                    pest::error::ErrorVariant::ParsingError {
                        ref positives,
                        ref negatives,
                    },
                location: ref input_location,
                ..
            } => match (&positives[..], &negatives[..]) {
                ([r, ..], _) if discriminant(r) == discriminant(&root_type) => {
                    Err(ParseError::NoMatch)
                } // parser couldnt descend further than root
                ([_, ..], _) => {
                    // not root so parser has descended but input is either
                    //  incomplete or incorrect
                    let end = match input_location {
                        InputLocation::Pos(offset) => offset,
                        InputLocation::Span((_, end)) => end,
                    };
                    if *end >= str.len() {
                        // awaiting more bytes to match
                        Err(ParseError::Incomplete)
                    } else {
                        // some more bytes are already there but they dont match
                        // which means we stopped at an incorrect pattern
                        // and cannot progress further
                        Err(ParseError::Parser(e))
                    }
                }
                _ => Err(ParseError::Parser(e)),
            },
            _ => Err(ParseError::Parser(e)),
        },
        Ok(mut pairs) => try_parse_message(pairs.next().ok_or(ParseError::Incomplete)?),
    }
}

pub(crate) fn try_parse_message(pair: Pair<Rule>) -> Result<NRTMMessage, ParseError> {
    fn try_parse_serial_from(serial: &Pair<'_, Rule>) -> Result<u64, ParseError> {
        let span = serial.as_span();
        serial
            .as_str()
            .parse()
            .map_err(|e| ParseError::MalformedSerial(span.into(), e))
    }

    fn no_leading_garbage_or_err(leading_garbage: &Pair<Rule>) -> Result<(), ParseError> {
        let span = leading_garbage.as_span();
        if span.start() == span.end() {
            Ok(())
        } else {
            Err(ParseError::LeadingGarbage(span.into()))
        }
    }

    match pair.as_rule() {
        Rule::v2_operation => {
            let mut inner_rules = pair.into_inner();
            let leading_garbage = inner_rules.next().ok_or(ParseError::NoMatch)?;
            no_leading_garbage_or_err(&leading_garbage)?;
            return try_parse_message(inner_rules.next().ok_or(ParseError::NoMatch)?);
        }
        Rule::v3_operation => {
            let mut inner_rules = pair.into_inner();
            let leading_garbage = inner_rules.next().ok_or(ParseError::NoMatch)?;
            no_leading_garbage_or_err(&leading_garbage)?;
            return try_parse_message(inner_rules.next().ok_or(ParseError::NoMatch)?);
        }
        Rule::v3_add_operation => {
            let span = pair.as_span();
            let mut inner_rules = pair.into_inner();
            let serial_pair = inner_rules.next().ok_or(ParseError::NoMatch)?;
            let serial: u64 = try_parse_serial_from(&serial_pair)?;
            let rpsl = inner_rules.next().ok_or(ParseError::NoMatch)?.as_str();

            return Ok(NRTMMessage {
                update: OpType::V3(Verb::ADD, serial),
                rpsl: String::from(rpsl),
                span: span.into(),
            });
        }
        Rule::v3_del_operation => {
            let span = pair.as_span();
            let mut inner_rules = pair.into_inner();
            let serial_pair = inner_rules.next().ok_or(ParseError::NoMatch)?;
            let serial: u64 = try_parse_serial_from(&serial_pair)?;
            let rpsl = inner_rules.next().ok_or(ParseError::NoMatch)?.as_str();

            return Ok(NRTMMessage {
                update: OpType::V3(Verb::DEL, serial),
                rpsl: String::from(rpsl),
                span: span.into(),
            });
        }
        Rule::v2_add_operation => {
            let span = pair.as_span();
            let mut inner_rules = pair.into_inner();
            let rpsl = inner_rules.next().ok_or(ParseError::NoMatch)?.as_str();

            return Ok(NRTMMessage {
                update: OpType::V2(Verb::ADD),
                rpsl: String::from(rpsl),
                span: span.into(),
            });
        }
        Rule::v2_del_operation => {
            let span = pair.as_span();
            let mut inner_rules = pair.into_inner();
            let rpsl = inner_rules.next().ok_or(ParseError::NoMatch)?.as_str();

            return Ok(NRTMMessage {
                update: OpType::V2(Verb::DEL),
                rpsl: String::from(rpsl),
                span: span.into(),
            });
        }
        _ => {}
    };

    Err(ParseError::NoMatch)
}
