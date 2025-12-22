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

mod pipeline;

pub use pipeline::{LineTemplate, Pipeline, PipelineStage};

use pipeline::{
    DropStage, JsonPath, JsonSelector, JsonStage, LabelFormatRule, LabelFormatStage,
    LabelFormatValue,
};

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_until, take_while, take_while1},
    character::complete::{char, multispace0, multispace1, none_of},
    combinator::{all_consuming, cut, map, map_res, opt, recognize},
    error::{Error as NomError, context},
    multi::{fold_many0, fold_many1, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded},
};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct LogqlExpr {
    pub selectors: Vec<LabelMatcher>,
    pub filters: Vec<LineFilter>,
    pub pipeline: Pipeline,
}

#[derive(Debug, Clone)]
pub struct MetricExpr {
    pub range: RangeExpr,
    pub aggregation: Option<VectorAggregation>,
}

#[derive(Debug, Clone)]
pub struct RangeExpr {
    pub function: RangeFunction,
    pub selector: LogqlExpr,
    pub duration: DurationValue,
}

#[derive(Debug, Clone, Copy)]
pub enum RangeFunction {
    CountOverTime,
    Rate,
}

#[derive(Debug, Clone)]
pub struct VectorAggregation {
    pub op: VectorAggregationOp,
    pub grouping: Option<GroupModifier>,
}

#[derive(Debug, Clone, Copy)]
pub enum VectorAggregationOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

#[derive(Debug, Clone)]
pub enum GroupModifier {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, Copy)]
pub struct DurationValue {
    nanoseconds: i64,
}

impl DurationValue {
    pub fn new(nanoseconds: i64) -> Result<Self, String> {
        if nanoseconds <= 0 {
            return Err("range duration must be positive".into());
        }
        Ok(Self { nanoseconds })
    }

    pub fn as_nanoseconds(&self) -> i64 {
        self.nanoseconds
    }

    pub fn parse_literal(input: &str) -> Result<Self, String> {
        let mut total: i128 = 0;
        let mut rest = input.trim();
        if rest.is_empty() {
            return Err("duration literal cannot be empty".into());
        }
        while !rest.is_empty() {
            let (value, unit_len) = parse_duration_segment(rest)?;
            total = total
                .checked_add(value as i128)
                .ok_or_else(|| "duration value overflowed".to_string())?;
            rest = &rest[unit_len..];
            rest = rest.trim_start();
        }
        let nanos: i64 = total
            .try_into()
            .map_err(|_| "duration exceeds supported range".to_string())?;
        DurationValue::new(nanos)
    }
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

    pub fn parse_metric(&self, input: &str) -> Result<Option<MetricExpr>, LogqlError> {
        parse_metric_expr(input)
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

fn parse_metric_expr(input: &str) -> Result<Option<MetricExpr>, LogqlError> {
    if !looks_like_metric(input) {
        return Ok(None);
    }
    all_consuming(delimited(multispace0, metric_expression, multispace0))
        .parse(input)
        .map(|(_, expr)| Some(expr))
        .map_err(|err| LogqlError::Invalid(err.to_string()))
}

fn looks_like_metric(input: &str) -> bool {
    let trimmed = input.trim_start();
    let ident = trimmed
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase();
    if ident.is_empty() {
        return false;
    }
    let metric_tokens = [
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "rate",
        "count_over_time",
    ];
    metric_tokens.iter().any(|token| *token == ident)
}

fn query(input: &str) -> NomResult<'_, LogqlExpr> {
    let (input, selectors) = selector(input)?;
    let (input, filters) = many_filters(input)?;
    let (input, pipeline) = pipeline_expr(input)?;
    Ok((
        input,
        LogqlExpr {
            selectors,
            filters,
            pipeline,
        },
    ))
}

fn metric_expression(input: &str) -> NomResult<'_, MetricExpr> {
    alt((
        vector_aggregation_expr,
        map(range_function_expr, |range| MetricExpr {
            range,
            aggregation: None,
        }),
    ))
    .parse(input)
}

fn vector_aggregation_expr(input: &str) -> NomResult<'_, MetricExpr> {
    map(
        pair(
            aggregation_op,
            pair(opt(group_modifier), aggregation_argument),
        ),
        |(op, (grouping, range))| MetricExpr {
            range,
            aggregation: Some(VectorAggregation { op, grouping }),
        },
    )
    .parse(input)
}

fn aggregation_argument(input: &str) -> NomResult<'_, RangeExpr> {
    delimited(
        preceded(multispace0, char('(')),
        cut(range_function_expr),
        preceded(multispace0, char(')')),
    )
    .parse(input)
}

fn aggregation_op(input: &str) -> NomResult<'_, VectorAggregationOp> {
    context(
        "aggregation operator",
        preceded(
            multispace0,
            alt((
                map(tag("sum"), |_| VectorAggregationOp::Sum),
                map(tag("avg"), |_| VectorAggregationOp::Avg),
                map(tag("min"), |_| VectorAggregationOp::Min),
                map(tag("max"), |_| VectorAggregationOp::Max),
                map(tag("count"), |_| VectorAggregationOp::Count),
            )),
        ),
    )
    .parse(input)
}

fn group_modifier(input: &str) -> NomResult<'_, GroupModifier> {
    context(
        "group modifier",
        map(
            pair(
                preceded(multispace1, alt((tag("by"), tag("without")))),
                delimited(
                    preceded(multispace0, char('(')),
                    cut(label_list),
                    preceded(multispace0, char(')')),
                ),
            ),
            |(modifier, labels)| match modifier {
                "by" => GroupModifier::By(labels),
                "without" => GroupModifier::Without(labels),
                _ => unreachable!(),
            },
        ),
    )
    .parse(input)
}

fn label_list(input: &str) -> NomResult<'_, Vec<String>> {
    separated_list1(
        preceded(multispace0, char(',')),
        preceded(multispace0, label_identifier),
    )
    .parse(input)
}

fn range_function_expr(input: &str) -> NomResult<'_, RangeExpr> {
    map(
        pair(
            range_function,
            delimited(
                preceded(multispace0, char('(')),
                cut(pair(query, preceded(multispace0, range_selector))),
                preceded(multispace0, char(')')),
            ),
        ),
        |(function, (selector, duration))| RangeExpr {
            function,
            selector,
            duration,
        },
    )
    .parse(input)
}

fn range_function(input: &str) -> NomResult<'_, RangeFunction> {
    context(
        "range function",
        preceded(
            multispace0,
            alt((
                map(tag("count_over_time"), |_| RangeFunction::CountOverTime),
                map(tag("rate"), |_| RangeFunction::Rate),
            )),
        ),
    )
    .parse(input)
}

fn range_selector(input: &str) -> NomResult<'_, DurationValue> {
    context(
        "range selector",
        delimited(char('['), cut(duration_literal), char(']')),
    )
    .parse(input)
}

fn duration_literal(input: &str) -> NomResult<'_, DurationValue> {
    context(
        "duration literal",
        map_res(
            recognize(fold_many1(duration_segment_token, || (), |_, _| ())),
            DurationValue::parse_literal,
        ),
    )
    .parse(input)
}

fn duration_segment_token(input: &str) -> NomResult<'_, ()> {
    map(
        pair(
            take_while1(|ch: char| ch.is_ascii_digit()),
            alt((
                tag("ns"),
                tag("us"),
                tag("µs"),
                tag("ms"),
                tag("s"),
                tag("m"),
                tag("h"),
                tag("d"),
                tag("w"),
            )),
        ),
        |_| (),
    )
    .parse(input)
}

fn parse_duration_segment(input: &str) -> Result<(i64, usize), String> {
    if input.is_empty() {
        return Err("duration literal cannot be empty".into());
    }
    let mut digit_len = 0;
    for ch in input.chars() {
        if ch.is_ascii_digit() {
            digit_len += ch.len_utf8();
        } else {
            break;
        }
    }
    if digit_len == 0 {
        return Err("duration segment is missing digits".into());
    }
    let number = input[..digit_len]
        .parse::<i64>()
        .map_err(|_| "duration value is not a valid integer".to_string())?;
    let rest = &input[digit_len..];
    let (multiplier, unit_len) = duration_unit_multiplier(rest)
        .ok_or_else(|| "duration segment is missing a valid unit".to_string())?;
    let total = number
        .checked_mul(multiplier)
        .ok_or_else(|| "duration value overflowed".to_string())?;
    Ok((total, digit_len + unit_len))
}

fn duration_unit_multiplier(input: &str) -> Option<(i64, usize)> {
    let units: [(&str, i64); 9] = [
        ("ns", 1),
        ("us", 1_000),
        ("µs", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("h", 3_600 * 1_000_000_000),
        ("d", 86_400 * 1_000_000_000),
        ("w", 604_800 * 1_000_000_000),
    ];
    for (token, multiplier) in units {
        if input.starts_with(token) {
            return Some((multiplier, token.len()));
        }
    }
    None
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

fn pipeline_expr(input: &str) -> NomResult<'_, Pipeline> {
    map(
        fold_many0(pipeline_stage, Vec::new, |mut stages, stage| {
            stages.push(stage);
            stages
        }),
        Pipeline::new,
    )
    .parse(input)
}

fn pipeline_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "pipeline stage",
        preceded(
            multispace0,
            preceded(
                char('|'),
                preceded(
                    multispace1,
                    alt((
                        logfmt_stage,
                        json_stage,
                        line_format_stage,
                        label_format_stage,
                        drop_stage,
                    )),
                ),
            ),
        ),
    )
    .parse(input)
}

fn logfmt_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "logfmt stage",
        map(tag("logfmt"), |_| PipelineStage::Logfmt),
    )
    .parse(input)
}

fn line_format_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "line_format stage",
        map_res(
            preceded(tag("line_format"), preceded(multispace1, string_literal)),
            |template| LineTemplate::compile(template).map(PipelineStage::LineFormat),
        ),
    )
    .parse(input)
}

fn json_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "json stage",
        map(
            pair(
                tag("json"),
                opt(preceded(
                    multispace1,
                    separated_list1(
                        preceded(multispace0, char(',')),
                        preceded(multispace0, json_selector),
                    ),
                )),
            ),
            |(_, selectors)| match selectors {
                Some(selectors) => PipelineStage::Json(JsonStage::Selectors(selectors)),
                None => PipelineStage::Json(JsonStage::All),
            },
        ),
    )
    .parse(input)
}

fn json_selector(input: &str) -> NomResult<'_, JsonSelector> {
    context(
        "json selector",
        map_res(
            pair(
                label_identifier,
                opt(preceded(
                    multispace0,
                    preceded(char('='), preceded(multispace0, string_literal)),
                )),
            ),
            |(label, expr)| {
                let expression = expr.unwrap_or_else(|| label.clone());
                JsonPath::parse(&expression).map(|path| JsonSelector {
                    target: label,
                    path,
                })
            },
        ),
    )
    .parse(input)
}

fn label_format_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "label_format stage",
        map(
            preceded(
                tag("label_format"),
                preceded(
                    multispace1,
                    separated_list1(
                        preceded(multispace0, char(',')),
                        preceded(multispace0, label_format_rule),
                    ),
                ),
            ),
            |rules| PipelineStage::LabelFormat(LabelFormatStage { rules }),
        ),
    )
    .parse(input)
}

fn drop_stage(input: &str) -> NomResult<'_, PipelineStage> {
    context(
        "drop stage",
        map(
            preceded(
                tag("drop"),
                preceded(
                    multispace1,
                    separated_list1(
                        preceded(multispace0, char(',')),
                        preceded(multispace0, label_identifier),
                    ),
                ),
            ),
            |targets| PipelineStage::Drop(DropStage { targets }),
        ),
    )
    .parse(input)
}

fn label_format_rule(input: &str) -> NomResult<'_, LabelFormatRule> {
    context(
        "label format rule",
        map(
            pair(
                label_identifier,
                preceded(
                    multispace0,
                    preceded(
                        char('='),
                        preceded(
                            multispace0,
                            alt((
                                map(label_identifier, LabelFormatValue::Source),
                                map_res(string_literal, |template| {
                                    LineTemplate::compile(template).map(LabelFormatValue::Template)
                                }),
                            )),
                        ),
                    ),
                ),
            ),
            |(target, value)| LabelFormatRule { target, value },
        ),
    )
    .parse(input)
}

fn label_identifier(input: &str) -> NomResult<'_, String> {
    map(recognize(pair(label_start, label_chars)), |ident: &str| {
        ident.to_string()
    })
    .parse(input)
}

fn string_literal(input: &str) -> NomResult<'_, String> {
    context(
        "string literal",
        alt((double_quoted_literal, backtick_literal)),
    )
    .parse(input)
}

fn double_quoted_literal(input: &str) -> NomResult<'_, String> {
    delimited(
        char('"'),
        cut(fold_many0(
            alt((unescaped_char, escaped_char)),
            String::new,
            |mut acc, item| {
                acc.push(item);
                acc
            },
        )),
        char('"'),
    )
    .parse(input)
}

fn backtick_literal(input: &str) -> NomResult<'_, String> {
    delimited(
        char('`'),
        cut(map(take_until("`"), |value: &str| value.to_string())),
        char('`'),
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

    #[test]
    fn parse_pipeline_stages() {
        let expr = LogqlParser
            .parse("{container=\"frontend\"} | logfmt | line_format \"{{.query}} {{.duration}}\"")
            .unwrap();
        assert_eq!(expr.pipeline.stages().len(), 2);
        assert!(matches!(expr.pipeline.stages()[0], PipelineStage::Logfmt));
        assert!(matches!(
            expr.pipeline.stages()[1],
            PipelineStage::LineFormat(_)
        ));
    }

    #[test]
    fn parse_json_stage_with_selectors() {
        let expr = LogqlParser
            .parse("{job=\"api\"} | json duration=\"data.latency\", ua=\"request[\\\"ua\\\"]\"")
            .unwrap();
        assert_eq!(expr.pipeline.stages().len(), 1);
        match &expr.pipeline.stages()[0] {
            PipelineStage::Json(JsonStage::Selectors(fields)) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].target, "duration");
                assert_eq!(fields[1].target, "ua");
            }
            _ => panic!("expected json stage with selectors"),
        }
    }

    #[test]
    fn parse_label_format_stage() {
        let expr = LogqlParser
            .parse("{job=\"api\"} | label_format level=severity,new_label=\"{{.app}}\"")
            .unwrap();
        assert_eq!(expr.pipeline.stages().len(), 1);
        match &expr.pipeline.stages()[0] {
            PipelineStage::LabelFormat(stage) => {
                assert_eq!(stage.rules.len(), 2);
                assert_eq!(stage.rules[0].target, "level");
            }
            _ => panic!("expected label_format stage"),
        }
    }

    #[test]
    fn parse_drop_stage() {
        let expr = LogqlParser
            .parse("{job=\"api\"} | drop __error__,temp_label")
            .unwrap();
        assert_eq!(expr.pipeline.stages().len(), 1);
        match &expr.pipeline.stages()[0] {
            PipelineStage::Drop(stage) => {
                assert_eq!(
                    stage.targets,
                    vec!["__error__".to_string(), "temp_label".to_string()]
                );
            }
            other => panic!("unexpected stage: {other:?}"),
        }
    }

    #[test]
    fn metric_drop_labels_permit_only_drop_stage() {
        let expr = LogqlParser
            .parse("{job=\"api\"} | drop __error__,temp_label | drop foo")
            .unwrap();
        let drops = expr.pipeline.metric_drop_labels().unwrap();
        assert_eq!(
            drops,
            vec![
                "__error__".to_string(),
                "foo".to_string(),
                "temp_label".to_string()
            ]
        );
    }

    #[test]
    fn metric_drop_labels_rejects_other_stages() {
        let expr = LogqlParser
            .parse("{job=\"api\"} | json | drop __error__")
            .unwrap();
        let err = expr.pipeline.metric_drop_labels().unwrap_err();
        assert!(err.contains("drop"), "unexpected error: {}", err);
    }

    #[test]
    fn duration_literal_with_multiple_segments() {
        let value = DurationValue::parse_literal("1h30m15s").unwrap();
        assert_eq!(
            value.as_nanoseconds(),
            (3_600 + 30 * 60 + 15) * 1_000_000_000
        );
        let short = DurationValue::parse_literal("10ms").unwrap();
        assert_eq!(short.as_nanoseconds(), 10_000_000);
    }

    #[test]
    fn duration_literal_rejects_invalid_units() {
        assert!(DurationValue::parse_literal("").is_err());
        assert!(DurationValue::parse_literal("10x").is_err());
        assert!(DurationValue::parse_literal("ms").is_err());
    }

    #[test]
    fn parse_metric_rate_without_aggregation() {
        let parsed = LogqlParser
            .parse_metric("rate({app=\"api\",env!=\"prod\"}[5m])")
            .unwrap()
            .unwrap();
        assert!(matches!(parsed.range.function, RangeFunction::Rate));
        assert_eq!(parsed.range.selector.selectors.len(), 2);
        assert!(parsed.range.selector.pipeline.is_empty());
        assert_eq!(
            parsed.range.duration.as_nanoseconds(),
            5 * 60 * 1_000_000_000
        );
        assert!(parsed.aggregation.is_none());
    }

    #[test]
    fn parse_metric_sum_by_labels() {
        let parsed = LogqlParser
            .parse_metric("sum by (app,instance) (count_over_time({job=\"svc\"}[1h]))")
            .unwrap()
            .unwrap();
        let agg = parsed.aggregation.unwrap();
        match agg.grouping {
            Some(GroupModifier::By(labels)) => {
                assert_eq!(labels, vec!["app".to_string(), "instance".to_string()])
            }
            other => panic!("unexpected grouping: {other:?}"),
        }
        assert!(matches!(agg.op, VectorAggregationOp::Sum));
        assert_eq!(
            parsed.range.duration.as_nanoseconds(),
            3_600 * 1_000_000_000
        );
    }

    #[test]
    fn parse_metric_non_metric_returns_none() {
        assert!(
            LogqlParser
                .parse_metric("{app=\"loki\"}")
                .unwrap()
                .is_none()
        );
    }
}
