use crate::{NRTMParser, OpType, ParseError, PestNRTMParser, Rule, Verb};
use pest::Parser;
use std::assert_matches;

#[test]
fn parse_v2_add_msg() {
    let rpsl = "\
some: property
and: another one
with: double lf ending

";
    let nrtmv2 = format!(
        "\
ADD

{}",
        rpsl
    );

    let pairs = PestNRTMParser::parse(Rule::v2_operation, nrtmv2.as_str())
        .expect("parsing nrtm message failed")
        .next()
        .unwrap();
    let parsed = crate::try_parse_message(pairs).unwrap();
    assert_matches!(parsed.update, OpType::V2(Verb::ADD));
    assert_eq!(parsed.rpsl, rpsl);
    // check that span is correct
    assert_eq!(parsed.span.start_b, 0);
    assert_eq!(parsed.span.end_b, nrtmv2.len());
}

#[test]
fn parse_v3_add_msg() {
    let rpsl = "\
some: property
and: another one
with: double lf ending

";
    let nrtmv3 = format!(
        "\
ADD 666666

{}",
        rpsl
    );

    let pairs = PestNRTMParser::parse(Rule::v3_operation, nrtmv3.as_str())
        .expect("parsing message failed")
        .next()
        .unwrap();
    let parsed = crate::try_parse_message(pairs).unwrap();
    assert_matches!(parsed.update, OpType::V3(Verb::ADD, 666_666));
    assert_eq!(parsed.rpsl, rpsl);
    // check that span is correct
    assert_eq!(parsed.span.start_b, 0);
    assert_eq!(parsed.span.end_b, nrtmv3.len());
}

#[test]
fn check_spans() {
    let nrtmv3 = "\
ADD 666666

some: property
and: another one
with: double lf ending

ADD 666667

some: property
and: another one
with: double lf ending

";

    let res = crate::NRTMV3Parser::try_parse(nrtmv3).unwrap();
    // check that span is correct
    assert_eq!(res.span.start_b, nrtmv3.find("ADD").unwrap());
    assert_eq!(res.span.end_b, nrtmv3.rfind("ADD").unwrap());
}

#[test]
fn signal_leading_garbage() {
    let nrtmv3 = "\
\\*xxtsome: leading garbage
a-malformed: object

ADD 666666

some: property
and: another one
with: double lf ending

ADD 666667

some: property
and: another one
with: double lf ending

";

    let e = crate::NRTMV3Parser::try_parse(nrtmv3).expect_err("parse error expected");
    match e {
        ParseError::LeadingGarbage(span) => {
            assert_eq!(span.start_b, 0);
            assert_eq!(span.end_b, nrtmv3.find("ADD").unwrap());
            assert_eq!(
                span.str,
                "\\*xxtsome: leading garbage\na-malformed: object\n\n"
            );
        }
        any => {
            panic!("got wrong error type {:?}", any)
        }
    }
}

#[test]
fn signal_incomplete_nrtmv3() {
    let incomplete_nrtmv3 = "\
ADD 666666

some: property
and: another one
# keine endlich
# kei
";

    let res = crate::NRTMV3Parser::try_parse(incomplete_nrtmv3);

    assert_matches!(res, Err(ParseError::Incomplete));
}

#[test]
fn signal_incomplete_nrtmv2() {
    let incomplete_nrtmv2 = "\
ADD

some: property
and: another one
# keine endlich
# kei
";

    let res = crate::NRTMV2Parser::try_parse(incomplete_nrtmv2);

    assert_matches!(res, Err(ParseError::Incomplete));
}

#[test]
fn signal_broken_nrtmv2() {
    let broken_nrtmv2 = "\
ADD

some: property
\\xxtand: another one with an incorrect syntax
plus-some: stuff afterwards

";

    let res = crate::NRTMV2Parser::try_parse(broken_nrtmv2);

    assert_matches!(res, Err(ParseError::Parser(_)));
}

#[test]
fn signal_no_match() {
    let no_match_nrtmv2 = "\
lsdjkflkjsdlfkjsldkjf;lsd
sdlkfjsdlkjfljsdf
sdlfjlsdkf

";

    let res = crate::NRTMV3Parser::try_parse(no_match_nrtmv2);

    // technically, with garbage-proof grammar, leading garbage with no match
    // will always be intepreted as incomplete, as more append *could* result
    // in a match
    assert_matches!(res, Err(ParseError::Incomplete));
}
