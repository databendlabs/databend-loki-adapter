// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, multispace0, none_of},
    combinator::{all_consuming, cut, map, recognize},
    error::{Error as NomError, context},
    multi::{fold_many0, separated_list0},
    sequence::{delimited, pair, preceded, terminated},
};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct LogqlExpr {
    pub selectors: Vec<LabelMatcher>,
    pub filters: Vec<LineFilter>,
}

#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub key: String,
    pub op: LabelOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy)]
pub enum LabelOp {
    Eq,
    NotEq,
    RegexEq,
    RegexNotEq,
}

#[derive(Debug, Clone)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
}

#[derive(Debug, Default, Clone)]
pub struct LogqlParser;

impl LogqlParser {
    pub fn parse(&self, input: &str) -> Result<LogqlExpr, LogqlError> {
        parse_logql(input)
    }
}

#[derive(Debug, Error)]
pub enum LogqlError {
    #[error("failed to parse LogQL: {0}")]
    Invalid(String),
}

type NomResult<'a, T> = IResult<&'a str, T, NomError<&'a str>>;

fn parse_logql(input: &str) -> Result<LogqlExpr, LogqlError> {
    all_consuming(delimited(multispace0, query, multispace0))
        .parse(input)
        .map(|(_, expr)| expr)
        .map_err(|err| LogqlError::Invalid(err.to_string()))
}

fn query(input: &str) -> NomResult<'_, LogqlExpr> {
    let (input, selectors) = selector(input)?;
    let (input, filters) = many_filters(input)?;
    Ok((input, LogqlExpr { selectors, filters }))
}

fn selector(input: &str) -> NomResult<'_, Vec<LabelMatcher>> {
    context(
        "label selector",
        delimited(
            preceded(multispace0, char('{')),
            separated_list0(
                preceded(multispace0, char(',')),
                preceded(multispace0, label_matcher),
            ),
            preceded(multispace0, char('}')),
        ),
    )
    .parse(input)
}

fn label_matcher(input: &str) -> NomResult<'_, LabelMatcher> {
    context(
        "label matcher",
        map(
            context("label key", recognize(pair(label_start, label_chars)))
                .and(preceded(
                    multispace0,
                    context(
                        "label operator",
                        alt((tag("=~"), tag("!~"), tag("!="), tag("="))),
                    ),
                ))
                .and(preceded(multispace0, string_literal)),
            |((key, op_str), value)| {
                let op = match op_str {
                    "=~" => LabelOp::RegexEq,
                    "!~" => LabelOp::RegexNotEq,
                    "!=" => LabelOp::NotEq,
                    "=" => LabelOp::Eq,
                    _ => unreachable!(),
                };
                LabelMatcher {
                    key: key.to_string(),
                    op,
                    value,
                }
            },
        ),
    )
    .parse(input)
}

fn many_filters(input: &str) -> NomResult<'_, Vec<LineFilter>> {
    let mut rest = input;
    let mut filters = Vec::new();
    loop {
        match line_filter(rest) {
            Ok((next, filter)) => {
                filters.push(filter);
                rest = next;
            }
            Err(nom::Err::Error(_)) => return Ok((rest, filters)),
            Err(err) => return Err(err),
        }
    }
}

fn line_filter(input: &str) -> NomResult<'_, LineFilter> {
    context(
        "line filter",
        preceded(
            multispace0,
            map(
                pair(
                    alt((tag("|="), tag("|~"), tag("!="), tag("!~"))),
                    preceded(multispace0, string_literal),
                ),
                |(op_str, value)| LineFilter {
                    op: match op_str {
                        "|=" => LineFilterOp::Contains,
                        "|~" => LineFilterOp::Regex,
                        "!=" => LineFilterOp::NotContains,
                        "!~" => LineFilterOp::NotRegex,
                        _ => unreachable!(),
                    },
                    value,
                },
            ),
        ),
    )
    .parse(input)
}

fn string_literal(input: &str) -> NomResult<'_, String> {
    context(
        "string literal",
        preceded(
            char('"'),
            cut(terminated(
                fold_many0(
                    alt((unescaped_char, escaped_char)),
                    String::new,
                    |mut acc, item| {
                        acc.push(item);
                        acc
                    },
                ),
                char('"'),
            )),
        ),
    )
    .parse(input)
}

fn unescaped_char(input: &str) -> NomResult<'_, char> {
    map(none_of("\\\""), |c| c).parse(input)
}

fn escaped_char(input: &str) -> NomResult<'_, char> {
    preceded(
        char('\\'),
        alt((
            map(char('\\'), |_| '\\'),
            map(char('"'), |_| '"'),
            map(char('n'), |_| '\n'),
            map(char('r'), |_| '\r'),
            map(char('t'), |_| '\t'),
        )),
    )
    .parse(input)
}

fn label_start(input: &str) -> NomResult<'_, &str> {
    take_while1(is_label_start)(input)
}

fn label_chars(input: &str) -> NomResult<'_, &str> {
    take_while(is_label_char)(input)
}

fn is_label_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_label_char(ch: char) -> bool {
    is_label_start(ch) || ch.is_ascii_digit() || matches!(ch, ':' | '.' | '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_selector() {
        let expr = LogqlParser.parse("{app=\"loki\",env!=\"prod\"}").unwrap();
        assert_eq!(expr.selectors.len(), 2);
        assert_eq!(expr.filters.len(), 0);
        assert_eq!(expr.selectors[0].key, "app");
        assert!(matches!(expr.selectors[0].op, LabelOp::Eq));
        assert_eq!(expr.selectors[0].value, "loki");
        assert_eq!(expr.selectors[1].key, "env");
        assert!(matches!(expr.selectors[1].op, LabelOp::NotEq));
        assert_eq!(expr.selectors[1].value, "prod");
    }

    #[test]
    fn parse_with_filters_and_escaped_string() {
        let expr = LogqlParser
            .parse("{app=\"loki\"} |= \"error\\\"\" |~ \"warn\\n\" != \"drop\" !~ \"panic\"")
            .unwrap();
        assert_eq!(expr.filters.len(), 4);
        assert_eq!(expr.filters[0].value, "error\"");
        assert!(matches!(expr.filters[0].op, LineFilterOp::Contains));
        assert_eq!(expr.filters[1].value, "warn\n");
        assert!(matches!(expr.filters[1].op, LineFilterOp::Regex));
        assert!(matches!(expr.filters[2].op, LineFilterOp::NotContains));
        assert!(matches!(expr.filters[3].op, LineFilterOp::NotRegex));
    }

    #[test]
    fn parse_fail_on_incomplete_string() {
        let err = LogqlParser.parse("{app=\"loki}").unwrap_err();
        assert!(matches!(err, LogqlError::Invalid(_)));
    }

    #[test]
    fn parse_fail_on_missing_brace() {
        let err = LogqlParser.parse("{app=\"loki\"").unwrap_err();
        assert!(matches!(err, LogqlError::Invalid(_)));
    }
}
