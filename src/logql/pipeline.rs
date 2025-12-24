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

use std::{
    collections::{BTreeMap, HashMap},
    iter::Peekable,
    str::{Chars, FromStr},
};

use base64::{Engine as _, engine::general_purpose};
use byte_unit::Byte;
use chrono::{DateTime, FixedOffset, Local, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use humantime::parse_duration;
use once_cell::sync::Lazy;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use regex::Regex;
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    stages: Vec<PipelineStage>,
}

impl Pipeline {
    pub fn new(stages: Vec<PipelineStage>) -> Self {
        Self { stages }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn metric_drop_labels(&self) -> Result<Vec<String>, String> {
        let mut labels = Vec::new();
        for stage in &self.stages {
            match stage {
                PipelineStage::Drop(stage) => labels.extend(stage.targets.iter().cloned()),
                PipelineStage::LineFormat(_) => {
                    continue;
                }
                _ => {
                    return Err(
                        "metric queries only support `drop` stages inside the selector pipeline"
                            .into(),
                    );
                }
            }
        }
        labels.sort();
        labels.dedup();
        Ok(labels)
    }

    #[cfg(test)]
    pub fn stages(&self) -> &[PipelineStage] {
        &self.stages
    }

    pub fn process(
        &self,
        labels: &BTreeMap<String, String>,
        line: &str,
        timestamp_ns: i128,
    ) -> PipelineOutput {
        if self.stages.is_empty() {
            return PipelineOutput {
                labels: labels.clone(),
                line: line.to_string(),
            };
        }
        let mut ctx = StageContext::new(labels, line.to_string(), timestamp_ns);
        for stage in &self.stages {
            stage.apply(&mut ctx);
        }
        ctx.into_output()
    }
}

impl Pipeline {
    pub fn pushdown_line_field(&self) -> Option<&str> {
        if self.stages.len() != 1 {
            return None;
        }
        match &self.stages[0] {
            PipelineStage::LineFormat(template) => template.pushdown_field(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineOutput {
    pub labels: BTreeMap<String, String>,
    pub line: String,
}

#[derive(Debug, Clone)]
pub enum PipelineStage {
    Logfmt,
    Json(JsonStage),
    LineFormat(LineTemplate),
    LabelFormat(LabelFormatStage),
    Drop(DropStage),
}

impl PipelineStage {
    fn apply(&self, ctx: &mut StageContext<'_>) {
        match self {
            PipelineStage::Logfmt => ctx.extract_logfmt(),
            PipelineStage::Json(stage) => ctx.extract_json(stage),
            PipelineStage::LineFormat(template) => ctx.apply_template(template),
            PipelineStage::LabelFormat(stage) => ctx.format_labels(stage),
            PipelineStage::Drop(stage) => ctx.drop_labels(stage),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JsonStage {
    All,
    Selectors(Vec<JsonSelector>),
}

#[derive(Debug, Clone)]
pub struct JsonSelector {
    pub target: String,
    pub path: JsonPath,
}

#[derive(Debug, Clone)]
pub struct JsonPath {
    segments: Vec<JsonPathSegment>,
}

impl JsonPath {
    pub fn new(segments: Vec<JsonPathSegment>) -> Self {
        Self { segments }
    }

    pub fn parse(expression: &str) -> Result<Self, String> {
        let mut chars = expression.trim().chars().peekable();
        let mut segments = Vec::new();
        let mut current = String::new();
        while let Some(&ch) = chars.peek() {
            match ch {
                '.' => {
                    chars.next();
                    if !current.trim().is_empty() {
                        segments.push(JsonPathSegment::Field(current.trim().to_string()));
                        current.clear();
                    }
                }
                '[' => {
                    if !current.trim().is_empty() {
                        segments.push(JsonPathSegment::Field(current.trim().to_string()));
                        current.clear();
                    }
                    chars.next();
                    consume_ws(&mut chars);
                    if let Some('\"') = chars.peek() {
                        chars.next();
                        let mut text = String::new();
                        while let Some(ch) = chars.next() {
                            match ch {
                                '\"' => break,
                                '\\' => {
                                    if let Some(next) = chars.next() {
                                        text.push(next);
                                    }
                                }
                                _ => text.push(ch),
                            }
                        }
                        consume_ws(&mut chars);
                        if chars.next() != Some(']') {
                            return Err("json expression is missing closing `]`".into());
                        }
                        if text.is_empty() {
                            return Err("json expression field cannot be empty".into());
                        }
                        segments.push(JsonPathSegment::Field(text));
                    } else {
                        let mut digits = String::new();
                        while let Some(&digit) = chars.peek() {
                            if digit.is_ascii_digit() {
                                digits.push(digit);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if digits.is_empty() {
                            return Err("json expression expects array index".into());
                        }
                        consume_ws(&mut chars);
                        if chars.next() != Some(']') {
                            return Err("json expression is missing closing `]`".into());
                        }
                        let idx = digits
                            .parse::<usize>()
                            .map_err(|_| "json expression index is invalid".to_string())?;
                        segments.push(JsonPathSegment::Index(idx));
                    }
                }
                ' ' | '\n' | '\t' | '\r' => {
                    chars.next();
                }
                _ => {
                    current.push(ch);
                    chars.next();
                }
            }
        }
        if !current.trim().is_empty() {
            segments.push(JsonPathSegment::Field(current.trim().to_string()));
        }
        if segments.is_empty() {
            return Err("json expression must reference at least one field".into());
        }
        Ok(JsonPath::new(segments))
    }

    fn evaluate<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut current = value;
        for segment in &self.segments {
            match (segment, current) {
                (JsonPathSegment::Field(key), Value::Object(map)) => {
                    current = map.get(key)?;
                }
                (JsonPathSegment::Index(idx), Value::Array(items)) => {
                    current = items.get(*idx)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }
}

#[derive(Debug, Clone)]
pub enum JsonPathSegment {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone)]
pub struct LabelFormatStage {
    pub rules: Vec<LabelFormatRule>,
}

#[derive(Debug, Clone)]
pub struct LabelFormatRule {
    pub target: String,
    pub value: LabelFormatValue,
}

#[derive(Debug, Clone)]
pub enum LabelFormatValue {
    Template(LineTemplate),
    Source(String),
}

#[derive(Debug, Clone)]
pub struct DropStage {
    pub targets: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LineTemplate {
    segments: Vec<TemplateSegment>,
    pushdown_field: Option<String>,
}

impl LineTemplate {
    pub fn compile(source: String) -> Result<Self, String> {
        let mut segments = Vec::new();
        let mut start = 0;
        let bytes = source.as_bytes();
        while let Some(open) = find_subsequence(bytes, b"{{", start) {
            if open > start {
                segments.push(TemplateSegment::Literal(source[start..open].to_string()));
            }
            let inner_start = open + 2;
            let close = find_subsequence(bytes, b"}}", inner_start)
                .ok_or_else(|| "line_format template is missing closing `}}`".to_string())?;
            let token = source[inner_start..close].trim();
            if token.is_empty() {
                return Err("line_format placeholder cannot be empty".into());
            }
            let expr = TemplateExpr::parse(token)?;
            segments.push(TemplateSegment::Expr(expr));
            start = close + 2;
        }
        if start < source.len() {
            segments.push(TemplateSegment::Literal(source[start..].to_string()));
        }
        let pushdown_field = LineTemplate::detect_pushdown_field(&segments);
        Ok(Self {
            segments,
            pushdown_field,
        })
    }

    fn render(&self, ctx: &StageContext<'_>) -> String {
        let mut result = String::new();
        for segment in &self.segments {
            match segment {
                TemplateSegment::Literal(text) => result.push_str(text),
                TemplateSegment::Expr(expr) => {
                    if let Ok(value) = expr.evaluate(ctx) {
                        result.push_str(&value.into_string());
                    }
                }
            }
        }
        result
    }
}

impl LineTemplate {
    fn detect_pushdown_field(segments: &[TemplateSegment]) -> Option<String> {
        if segments.len() != 1 {
            return None;
        }
        match &segments[0] {
            TemplateSegment::Expr(expr) => expr.field_name().map(|field| field.to_string()),
            _ => None,
        }
    }

    pub fn pushdown_field(&self) -> Option<&str> {
        self.pushdown_field.as_deref()
    }
}

#[derive(Debug, Clone)]
enum TemplateSegment {
    Literal(String),
    Expr(TemplateExpr),
}

#[derive(Debug, Clone)]
struct TemplateExpr {
    first: StageCommand,
    rest: Vec<FunctionCall>,
}

#[derive(Debug, Clone)]
enum StageCommand {
    Value(ValueExpr),
    Function(FunctionCall),
}

#[derive(Debug, Clone)]
struct FunctionCall {
    name: String,
    args: Vec<ValueExpr>,
}

#[derive(Debug, Clone)]
enum ValueExpr {
    Field(String),
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Clone)]
enum TemplateValue {
    String(String),
    Number(f64),
    Bool(bool),
    Time(DateTime<FixedOffset>),
}

impl TemplateValue {
    fn into_string(self) -> String {
        match self {
            TemplateValue::String(value) => value,
            TemplateValue::Number(value) => {
                if value.fract() == 0.0 {
                    (value.trunc() as i64).to_string()
                } else {
                    value.to_string()
                }
            }
            TemplateValue::Bool(value) => value.to_string(),
            TemplateValue::Time(value) => value.to_rfc3339(),
        }
    }

    fn as_string(&self) -> String {
        match self {
            TemplateValue::String(value) => value.clone(),
            TemplateValue::Number(value) => {
                if value.fract() == 0.0 {
                    (value.trunc() as i64).to_string()
                } else {
                    value.to_string()
                }
            }
            TemplateValue::Bool(value) => value.to_string(),
            TemplateValue::Time(value) => value.to_rfc3339(),
        }
    }

    fn as_f64(&self) -> Result<f64, String> {
        match self {
            TemplateValue::Number(value) => Ok(*value),
            TemplateValue::String(value) => value
                .parse::<f64>()
                .map_err(|_| format!("unable to parse `{value}` as number")),
            TemplateValue::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
            TemplateValue::Time(_) => Err("time value cannot be converted to number".into()),
        }
    }

    fn as_i64(&self) -> Result<i64, String> {
        let value = self.as_f64()?;
        Ok(value.trunc() as i64)
    }

    fn as_time(&self) -> Result<DateTime<FixedOffset>, String> {
        match self {
            TemplateValue::Time(value) => Ok(*value),
            TemplateValue::String(value) => parse_time_string(value),
            TemplateValue::Number(value) => {
                let secs = value.trunc() as i128;
                let nanos = ((value.fract()) * 1_000_000_000f64) as i128;
                ns_to_datetime(secs * 1_000_000_000 + nanos)
                    .ok_or_else(|| "unable to convert numeric value to timestamp".into())
            }
            TemplateValue::Bool(_) => Err("boolean value cannot be converted to timestamp".into()),
        }
    }

    fn truthy(&self) -> bool {
        match self {
            TemplateValue::String(value) => !value.is_empty(),
            TemplateValue::Number(value) => *value != 0.0,
            TemplateValue::Bool(value) => *value,
            TemplateValue::Time(_) => true,
        }
    }
}

static TEMPLATE_FUNCTIONS: Lazy<TemplateFunctions> = Lazy::new(TemplateFunctions::new);

impl TemplateExpr {
    fn parse(input: &str) -> Result<Self, String> {
        let mut stages = Vec::new();
        for stage in input.split('|') {
            let trimmed = stage.trim();
            if !trimmed.is_empty() {
                stages.push(trimmed);
            }
        }
        if stages.is_empty() {
            return Err("template expression cannot be empty".into());
        }
        let mut iter = stages.into_iter();
        let first = StageCommand::parse(iter.next().unwrap(), true)?;
        let mut rest = Vec::new();
        for stage in iter {
            rest.push(match StageCommand::parse(stage, false)? {
                StageCommand::Function(func) => func,
                StageCommand::Value(_) => {
                    return Err("only functions are allowed after the first pipeline stage".into());
                }
            });
        }
        Ok(Self { first, rest })
    }

    fn evaluate(&self, ctx: &StageContext<'_>) -> Result<TemplateValue, String> {
        let mut current = match &self.first {
            StageCommand::Value(expr) => expr.evaluate(ctx),
            StageCommand::Function(func) => func.invoke(None, ctx)?,
        };
        for func in &self.rest {
            current = func.invoke(Some(current), ctx)?;
        }
        Ok(current)
    }

    fn field_name(&self) -> Option<&str> {
        if !self.rest.is_empty() {
            return None;
        }
        match &self.first {
            StageCommand::Value(ValueExpr::Field(name)) => Some(name.as_str()),
            _ => None,
        }
    }
}

impl StageCommand {
    fn parse(input: &str, allow_value: bool) -> Result<Self, String> {
        let tokens = tokenize_command(input)?;
        if tokens.is_empty() {
            return Err("empty pipeline stage".into());
        }
        if allow_value && let Some(value) = ValueExpr::try_from_token(&tokens[0]) {
            if tokens.len() > 1 {
                return Err("value stages cannot have additional arguments".into());
            }
            return Ok(StageCommand::Value(value));
        }
        let name = tokens[0].value.clone();
        let args = tokens[1..]
            .iter()
            .map(ValueExpr::require)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StageCommand::Function(FunctionCall { name, args }))
    }
}

impl FunctionCall {
    fn invoke(
        &self,
        input: Option<TemplateValue>,
        ctx: &StageContext<'_>,
    ) -> Result<TemplateValue, String> {
        let mut args = Vec::with_capacity(self.args.len());
        for expr in &self.args {
            args.push(expr.evaluate(ctx));
        }
        TEMPLATE_FUNCTIONS.call(&self.name, input, args, ctx)
    }
}

impl ValueExpr {
    fn try_from_token(token: &Token) -> Option<Self> {
        if token.quoted {
            return Some(ValueExpr::String(token.value.clone()));
        }
        if let Some(stripped) = token.value.strip_prefix('.') {
            return Some(ValueExpr::Field(stripped.to_string()));
        }
        match token.value.as_str() {
            "true" => Some(ValueExpr::Bool(true)),
            "false" => Some(ValueExpr::Bool(false)),
            _ => token.value.parse::<f64>().ok().map(ValueExpr::Number),
        }
    }

    fn require(token: &Token) -> Result<Self, String> {
        ValueExpr::try_from_token(token)
            .ok_or_else(|| format!("unexpected token `{}`", token.value))
    }

    fn evaluate(&self, ctx: &StageContext<'_>) -> TemplateValue {
        match self {
            ValueExpr::Field(name) => {
                TemplateValue::String(ctx.lookup(name).unwrap_or("").to_string())
            }
            ValueExpr::String(value) => TemplateValue::String(value.clone()),
            ValueExpr::Number(value) => TemplateValue::Number(*value),
            ValueExpr::Bool(value) => TemplateValue::Bool(*value),
        }
    }
}

type TemplateFunc = fn(
    &str,
    Option<TemplateValue>,
    Vec<TemplateValue>,
    &StageContext<'_>,
) -> Result<TemplateValue, String>;

struct TemplateFunctions {
    funcs: HashMap<&'static str, TemplateFunc>,
}

impl TemplateFunctions {
    fn new() -> Self {
        let mut funcs: HashMap<&'static str, TemplateFunc> = HashMap::new();
        funcs.insert("__line__", func_line);
        funcs.insert("__timestamp__", func_timestamp);
        funcs.insert("alignLeft", func_align_left);
        funcs.insert("alignRight", func_align_right);
        funcs.insert("add", func_add);
        funcs.insert("addf", func_addf);
        funcs.insert("b64enc", func_b64enc);
        funcs.insert("b64dec", func_b64dec);
        funcs.insert("bytes", func_bytes);
        funcs.insert("ceil", func_ceil);
        funcs.insert("contains", func_contains);
        funcs.insert("count", func_count);
        funcs.insert("date", func_date);
        funcs.insert("default", func_default);
        funcs.insert("div", func_div);
        funcs.insert("divf", func_divf);
        funcs.insert("duration", func_duration);
        funcs.insert("duration_seconds", func_duration_seconds);
        funcs.insert("eq", func_eq);
        funcs.insert("float64", func_float64);
        funcs.insert("floor", func_floor);
        funcs.insert("fromJson", func_from_json);
        funcs.insert("hasPrefix", func_has_prefix);
        funcs.insert("hasSuffix", func_has_suffix);
        funcs.insert("indent", func_indent);
        funcs.insert("int", func_int);
        funcs.insert("lower", func_lower);
        funcs.insert("max", func_max);
        funcs.insert("maxf", func_maxf);
        funcs.insert("min", func_min);
        funcs.insert("minf", func_minf);
        funcs.insert("mod", func_mod);
        funcs.insert("mulf", func_mulf);
        funcs.insert("mul", func_mul);
        funcs.insert("nindent", func_nindent);
        funcs.insert("now", func_now);
        funcs.insert("printf", func_printf);
        funcs.insert("regexReplaceAll", func_regex_replace_all);
        funcs.insert("regexReplaceAllLiteral", func_regex_replace_all_literal);
        funcs.insert("repeat", func_repeat);
        funcs.insert("replace", func_replace);
        funcs.insert("round", func_round);
        funcs.insert("sub", func_sub);
        funcs.insert("subf", func_subf);
        funcs.insert("substr", func_substr);
        funcs.insert("title", func_title);
        funcs.insert("toDate", func_to_date);
        funcs.insert("toDateInZone", func_to_date_in_zone);
        funcs.insert("trim", func_trim);
        funcs.insert("trimAll", func_trim_all);
        funcs.insert("trimPrefix", func_trim_prefix);
        funcs.insert("trimSuffix", func_trim_suffix);
        funcs.insert("trunc", func_trunc);
        funcs.insert("unixEpoch", func_unix_epoch);
        funcs.insert("unixEpochMillis", func_unix_epoch_millis);
        funcs.insert("unixEpochNanos", func_unix_epoch_nanos);
        funcs.insert("unixToTime", func_unix_to_time);
        funcs.insert("upper", func_upper);
        funcs.insert("urlencode", func_urlencode);
        funcs.insert("urldecode", func_urldecode);
        Self { funcs }
    }

    fn call(
        &self,
        name: &str,
        input: Option<TemplateValue>,
        args: Vec<TemplateValue>,
        ctx: &StageContext<'_>,
    ) -> Result<TemplateValue, String> {
        match self.funcs.get(name) {
            Some(func) => func(name, input, args, ctx),
            None => Err(format!("unknown template function `{name}`")),
        }
    }
}

#[derive(Debug)]
struct Token {
    value: String,
    quoted: bool,
}

fn tokenize_command(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut quote_char = '\0';
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if quoted {
            match ch {
                '\\' if quote_char == '"' => {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                c if c == quote_char => {
                    tokens.push(Token {
                        value: std::mem::take(&mut current),
                        quoted: true,
                    });
                    quoted = false;
                    quote_char = '\0';
                }
                _ => current.push(ch),
            }
            continue;
        }
        match ch {
            '"' | '`' => {
                if !current.is_empty() {
                    return Err("quotes must start at a token boundary".into());
                }
                quoted = true;
                quote_char = ch;
            }
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(Token {
                        value: std::mem::take(&mut current),
                        quoted: false,
                    });
                }
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err("unterminated quoted string".into());
    }
    if !current.is_empty() {
        tokens.push(Token {
            value: current,
            quoted: false,
        });
    }
    Ok(tokens)
}

fn take_value(
    func: &str,
    input: Option<TemplateValue>,
    args: &mut Vec<TemplateValue>,
) -> Result<TemplateValue, String> {
    if let Some(value) = input {
        Ok(value)
    } else if !args.is_empty() {
        Ok(args.remove(0))
    } else {
        Err(format!("{func} requires a value"))
    }
}

fn take_arg(func: &str, args: &mut Vec<TemplateValue>) -> Result<TemplateValue, String> {
    if args.is_empty() {
        Err(format!("{func} requires more arguments"))
    } else {
        Ok(args.remove(0))
    }
}

fn gather_values(input: Option<TemplateValue>, mut args: Vec<TemplateValue>) -> Vec<TemplateValue> {
    if let Some(value) = input {
        args.insert(0, value);
    }
    args
}

fn func_line(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    if input.is_some() || !args.is_empty() {
        return Err(format!("{func} does not accept arguments"));
    }
    Ok(TemplateValue::String(ctx.original_line().to_string()))
}

fn func_timestamp(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    if input.is_some() || !args.is_empty() {
        return Err(format!("{func} does not accept arguments"));
    }
    Ok(TemplateValue::Time(ctx.timestamp()))
}

fn func_now(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    if input.is_some() || !args.is_empty() {
        return Err(format!("{func} does not accept arguments"));
    }
    Ok(TemplateValue::Time(ctx.now()))
}

fn expect_usize(value: TemplateValue, func: &str) -> Result<usize, String> {
    let count = value.as_i64()?;
    if count < 0 {
        Err(format!("{func} requires a non-negative width"))
    } else {
        Ok(count as usize)
    }
}

fn expect_i64(value: TemplateValue) -> Result<i64, String> {
    value.as_i64()
}

fn clamp_index(index: i64, len: usize) -> usize {
    if index < 0 {
        0
    } else {
        std::cmp::min(index as usize, len)
    }
}

fn substring_between(text: String, start: i64, end: i64) -> String {
    if end <= 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return String::new();
    }
    let len = chars.len();
    let start_idx = clamp_index(start, len);
    let end_idx = clamp_index(end, len);
    if end_idx <= start_idx {
        return String::new();
    }
    chars[start_idx..end_idx].iter().collect()
}

fn truncate_text(text: String, count: i64) -> String {
    if count == 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return String::new();
    }
    if count > 0 {
        let end = std::cmp::min(count as usize, chars.len());
        chars[..end].iter().collect()
    } else {
        let take = std::cmp::min((-count) as usize, chars.len());
        chars[chars.len() - take..].iter().collect()
    }
}

fn indent_text(text: String, spaces: usize, newline: bool) -> String {
    let padding = " ".repeat(spaces);
    let mut result = String::new();
    if newline {
        result.push('\n');
    }
    let mut first = true;
    for line in text.split('\n') {
        if !first {
            result.push('\n');
        }
        result.push_str(&padding);
        result.push_str(line);
        first = false;
    }
    result
}

fn func_bytes(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    let parsed = Byte::from_str(&source)
        .or_else(|_| Byte::from_str(&source.to_uppercase()))
        .map_err(|_| format!("bytes failed to parse `{source}`"))?;
    Ok(TemplateValue::String(parsed.to_string()))
}

fn func_duration(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(format!("{func} requires a duration value"));
    }
    let negative = trimmed.starts_with('-');
    let body = trimmed.trim_start_matches(['-', '+']);
    let duration = parse_duration(body)
        .map_err(|_| format!("{func} failed to parse `{source}` as duration"))?;
    let mut seconds = duration.as_secs_f64();
    if negative {
        seconds = -seconds;
    }
    Ok(TemplateValue::Number(seconds))
}

fn func_duration_seconds(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    func_duration(func, input, args, ctx)
}

fn func_from_json(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    let value: Value =
        serde_json::from_str(&source).map_err(|err| format!("fromJson failed: {err}"))?;
    Ok(TemplateValue::String(value.to_string()))
}

fn func_printf(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let fmt = take_arg(func, &mut args)?.into_string();
    let mut values = Vec::new();
    if let Some(value) = input {
        values.push(value);
    }
    values.extend(args);
    let rendered = apply_printf(&fmt, &values)?;
    Ok(TemplateValue::String(rendered))
}

fn func_unix_epoch(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?;
    let dt = source.as_time()?;
    let seconds = dt
        .with_timezone(&FixedOffset::east_opt(0).unwrap())
        .timestamp();
    Ok(TemplateValue::String(seconds.to_string()))
}

fn func_unix_epoch_millis(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?;
    let dt = source.as_time()?;
    let millis = dt
        .with_timezone(&FixedOffset::east_opt(0).unwrap())
        .timestamp_millis();
    Ok(TemplateValue::String(millis.to_string()))
}

fn func_unix_epoch_nanos(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?;
    let dt = source.as_time()?;
    let nanos = dt
        .with_timezone(&FixedOffset::east_opt(0).unwrap())
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("{func} failed to compute nanoseconds"))?;
    Ok(TemplateValue::String(nanos.to_string()))
}

fn func_unix_to_time(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Err(format!("{func} requires epoch value"));
    }
    let len = trimmed.len();
    let value = trimmed
        .parse::<i128>()
        .map_err(|_| format!("unable to parse `{source}` as epoch"))?;
    let nanos = match len {
        5 => value * 86_400 * 1_000_000_000,
        10 => value * 1_000_000_000,
        13 => value * 1_000_000,
        16 => value * 1_000,
        19 => value,
        _ => {
            return Err(format!(
                "unable to parse `{source}` as epoch; unexpected length {len}"
            ));
        }
    };
    let dt = ns_to_datetime(nanos)
        .ok_or_else(|| format!("unable to convert `{source}` to timestamp"))?;
    Ok(TemplateValue::Time(dt))
}

fn func_date(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let layout = take_arg(func, &mut args)?.into_string();
    let value = take_value(func, input, &mut args)?;
    let dt = value.as_time()?;
    let formatted = format_with_layout(&dt, &layout)?;
    Ok(TemplateValue::String(formatted))
}

fn func_to_date(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let layout = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    let dt = parse_with_layout(&layout, &source, None)?;
    Ok(TemplateValue::Time(dt))
}

fn func_to_date_in_zone(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let layout = take_arg(func, &mut args)?.into_string();
    let zone_name = take_arg(func, &mut args)?.into_string();
    let zone = parse_zone(&zone_name)?;
    let source = take_value(func, input, &mut args)?.into_string();
    let dt = parse_with_layout(&layout, &source, Some(zone))?;
    Ok(TemplateValue::Time(dt))
}

fn func_lower(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::String(value.into_string().to_lowercase()))
}

fn func_upper(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::String(value.into_string().to_uppercase()))
}

fn func_title(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?.into_string();
    let mut result = String::new();
    let mut new_word = true;
    for ch in value.chars() {
        if ch.is_whitespace() {
            new_word = true;
            result.push(ch);
            continue;
        }
        if new_word {
            result.extend(ch.to_uppercase());
            new_word = false;
        } else {
            result.extend(ch.to_lowercase());
        }
    }
    Ok(TemplateValue::String(result))
}

fn func_trim(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(value.trim().to_string()))
}

fn func_trim_all(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let chars = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    let set: Vec<char> = chars.chars().collect();
    Ok(TemplateValue::String(
        source.trim_matches(|c| set.contains(&c)).to_string(),
    ))
}

fn func_trim_prefix(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let prefix = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    if source.starts_with(&prefix) {
        Ok(TemplateValue::String(source[prefix.len()..].to_string()))
    } else {
        Ok(TemplateValue::String(source))
    }
}

fn func_trim_suffix(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let suffix = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    if source.ends_with(&suffix) {
        let end = source.len() - suffix.len();
        Ok(TemplateValue::String(source[..end].to_string()))
    } else {
        Ok(TemplateValue::String(source))
    }
}

fn compile_regex(pattern: &str, func: &str) -> Result<Regex, String> {
    Regex::new(pattern).map_err(|err| format!("{func} failed to compile regex: {err}"))
}

fn func_count(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let pattern = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    let regex = compile_regex(&pattern, func)?;
    let count = regex.find_iter(&source).count();
    Ok(TemplateValue::Number(count as f64))
}

fn func_regex_replace_all(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let pattern = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    let replacement = take_arg(func, &mut args)?.into_string();
    let regex = compile_regex(&pattern, func)?;
    Ok(TemplateValue::String(
        regex.replace_all(&source, replacement.as_str()).to_string(),
    ))
}

fn func_regex_replace_all_literal(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let pattern = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    let replacement = take_arg(func, &mut args)?.into_string();
    let regex = compile_regex(&pattern, func)?;
    Ok(TemplateValue::String(
        regex
            .replace_all(&source, regex::NoExpand(&replacement))
            .to_string(),
    ))
}

fn func_default(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let default = take_arg(func, &mut args)?;
    let source = if let Some(value) = input {
        value
    } else if !args.is_empty() {
        args.remove(0)
    } else {
        TemplateValue::String(String::new())
    };
    if source.truthy() {
        Ok(source)
    } else {
        Ok(default)
    }
}

fn func_contains(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let needle = take_arg(func, &mut args)?.into_string();
    let haystack = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::Bool(haystack.contains(&needle)))
}

fn func_eq(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let left = if let Some(value) = input {
        value
    } else {
        take_arg(func, &mut args)?
    };
    let right = take_arg(func, &mut args)?;
    Ok(TemplateValue::Bool(left.as_string() == right.as_string()))
}

fn func_has_prefix(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let prefix = take_arg(func, &mut args)?.into_string();
    let haystack = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::Bool(haystack.starts_with(&prefix)))
}

fn func_has_suffix(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let suffix = take_arg(func, &mut args)?.into_string();
    let haystack = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::Bool(haystack.ends_with(&suffix)))
}

fn func_int(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::Number(value.as_i64()? as f64))
}

fn func_float64(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::Number(value.as_f64()?))
}

fn func_add(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let values = gather_values(input, args);
    if values.is_empty() {
        return Err(format!("{func} requires at least one value"));
    }
    let mut total: i64 = 0;
    for value in values {
        total = total
            .checked_add(expect_i64(value)?)
            .ok_or_else(|| format!("{func} overflowed while adding values"))?;
    }
    Ok(TemplateValue::Number(total as f64))
}

fn func_addf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let values = gather_values(input, args);
    if values.is_empty() {
        return Err(format!("{func} requires at least one value"));
    }
    let mut total = 0.0;
    for value in values {
        total += value.as_f64()?;
    }
    Ok(TemplateValue::Number(total))
}

fn func_sub(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result = expect_i64(values.remove(0))?;
    for value in values {
        result = result
            .checked_sub(expect_i64(value)?)
            .ok_or_else(|| format!("{func} overflowed while subtracting values"))?;
    }
    Ok(TemplateValue::Number(result as f64))
}

fn func_subf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result = values.remove(0).as_f64()?;
    for value in values {
        result -= value.as_f64()?;
    }
    Ok(TemplateValue::Number(result))
}

fn func_mul(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result: i64 = 1;
    for value in values {
        result = result
            .checked_mul(expect_i64(value)?)
            .ok_or_else(|| format!("{func} overflowed while multiplying values"))?;
    }
    Ok(TemplateValue::Number(result as f64))
}

fn func_mulf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result = 1.0;
    for value in values {
        result *= value.as_f64()?;
    }
    Ok(TemplateValue::Number(result))
}

fn func_div(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result = expect_i64(values.remove(0))?;
    for value in values {
        let divisor = expect_i64(value)?;
        if divisor == 0 {
            return Err(format!("{func} encountered division by zero"));
        }
        result /= divisor;
    }
    Ok(TemplateValue::Number(result as f64))
}

fn func_divf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args);
    if values.len() < 2 {
        return Err(format!("{func} requires at least two values"));
    }
    let mut result = values.remove(0).as_f64()?;
    for value in values {
        let divisor = value.as_f64()?;
        if divisor == 0.0 {
            return Err(format!("{func} encountered division by zero"));
        }
        result /= divisor;
    }
    Ok(TemplateValue::Number(result))
}

fn func_mod(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let left = take_value(func, input, &mut args)?;
    let right = take_arg(func, &mut args)?;
    let divisor = expect_i64(right)?;
    if divisor == 0 {
        return Err(format!("{func} encountered division by zero"));
    }
    let remainder = expect_i64(left)? % divisor;
    Ok(TemplateValue::Number(remainder as f64))
}

fn func_max(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args).into_iter();
    let Some(first) = values.next() else {
        return Err(format!("{func} requires at least one value"));
    };
    let mut max = expect_i64(first)?;
    for value in values {
        max = max.max(expect_i64(value)?);
    }
    Ok(TemplateValue::Number(max as f64))
}

fn func_maxf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args).into_iter();
    let Some(first) = values.next() else {
        return Err(format!("{func} requires at least one value"));
    };
    let mut max = first.as_f64()?;
    for value in values {
        max = max.max(value.as_f64()?);
    }
    Ok(TemplateValue::Number(max))
}

fn func_min(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args).into_iter();
    let Some(first) = values.next() else {
        return Err(format!("{func} requires at least one value"));
    };
    let mut min = expect_i64(first)?;
    for value in values {
        min = min.min(expect_i64(value)?);
    }
    Ok(TemplateValue::Number(min as f64))
}

fn func_minf(
    func: &str,
    input: Option<TemplateValue>,
    args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let mut values = gather_values(input, args).into_iter();
    let Some(first) = values.next() else {
        return Err(format!("{func} requires at least one value"));
    };
    let mut min = first.as_f64()?;
    for value in values {
        min = min.min(value.as_f64()?);
    }
    Ok(TemplateValue::Number(min))
}

fn func_ceil(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::Number(value.as_f64()?.ceil()))
}

fn func_floor(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?;
    Ok(TemplateValue::Number(value.as_f64()?.floor()))
}

fn func_round(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let value = take_value(func, input, &mut args)?.as_f64()?;
    let precision = take_arg(func, &mut args)?.as_i64()? as i32;
    let round_on = if args.is_empty() {
        0.5
    } else {
        take_arg(func, &mut args)?.as_f64()?
    };
    let factor = 10f64.powi(precision);
    let scaled = value * factor;
    let floor = scaled.floor();
    let result = if (scaled - floor) >= round_on {
        (floor + 1.0) / factor
    } else {
        floor / factor
    };
    Ok(TemplateValue::Number(result))
}
fn func_replace(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let old = take_arg(func, &mut args)?.into_string();
    let new = take_arg(func, &mut args)?.into_string();
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(source.replace(&old, &new)))
}

fn func_repeat(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let count = expect_usize(take_arg(func, &mut args)?, func)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(source.repeat(count)))
}

fn func_align_left(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let width = expect_usize(take_arg(func, &mut args)?, func)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(format!(
        "{source:<width$}",
        width = width
    )))
}

fn func_align_right(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let width = expect_usize(take_arg(func, &mut args)?, func)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(format!(
        "{source:>width$}",
        width = width
    )))
}

fn func_indent(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let spaces = expect_usize(take_arg(func, &mut args)?, func)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(indent_text(source, spaces, false)))
}

fn func_nindent(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let spaces = expect_usize(take_arg(func, &mut args)?, func)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(indent_text(source, spaces, true)))
}

fn func_substr(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let start = expect_i64(take_arg(func, &mut args)?)?;
    let end = expect_i64(take_arg(func, &mut args)?)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(substring_between(source, start, end)))
}

fn func_trunc(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let count = expect_i64(take_arg(func, &mut args)?)?;
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(truncate_text(source, count)))
}

fn func_urlencode(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(
        utf8_percent_encode(&source, NON_ALPHANUMERIC).to_string(),
    ))
}

fn func_urldecode(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    let decoded = percent_decode_str(&source)
        .decode_utf8()
        .map_err(|_| format!("urldecode received invalid UTF-8 in `{source}`"))?;
    Ok(TemplateValue::String(decoded.into_owned()))
}

fn func_b64enc(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.into_string();
    Ok(TemplateValue::String(
        general_purpose::STANDARD.encode(source),
    ))
}

fn func_b64dec(
    func: &str,
    input: Option<TemplateValue>,
    mut args: Vec<TemplateValue>,
    _ctx: &StageContext<'_>,
) -> Result<TemplateValue, String> {
    let source = take_value(func, input, &mut args)?.as_string();
    let decoded = general_purpose::STANDARD
        .decode(source.as_bytes())
        .map_err(|_| "b64dec failed to decode input".to_string())?;
    let text =
        String::from_utf8(decoded).map_err(|_| "b64dec produced invalid UTF-8".to_string())?;
    Ok(TemplateValue::String(text))
}

fn apply_printf(fmt: &str, values: &[TemplateValue]) -> Result<String, String> {
    let mut chars = fmt.chars().peekable();
    let mut result = String::new();
    let mut index = 0;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            result.push(ch);
            continue;
        }
        if matches!(chars.peek(), Some('%')) {
            chars.next();
            result.push('%');
            continue;
        }
        let mut left_align = false;
        let mut zero_pad = false;
        loop {
            match chars.peek() {
                Some('-') => {
                    left_align = true;
                    chars.next();
                }
                Some('0') => {
                    zero_pad = true;
                    chars.next();
                }
                _ => break,
            }
        }
        let width = parse_digits(&mut chars);
        let precision = if matches!(chars.peek(), Some('.')) {
            chars.next();
            parse_digits(&mut chars)
        } else {
            None
        };
        let spec = chars
            .next()
            .ok_or_else(|| "printf requires a verb".to_string())?;
        let value = values
            .get(index)
            .ok_or_else(|| "printf requires more arguments".to_string())?;
        index += 1;
        let formatted = match spec {
            's' | 'v' => format_string_value(value, width, precision, left_align),
            'd' | 'i' => format_int_value(value, width, left_align, zero_pad)?,
            'f' => format_float_value(value, width, precision, left_align, zero_pad)?,
            _ => return Err(format!("unsupported printf verb `{spec}`")),
        };
        result.push_str(&formatted);
    }
    Ok(result)
}

fn parse_digits<I>(chars: &mut Peekable<I>) -> Option<usize>
where
    I: Iterator<Item = char>,
{
    let mut digits = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn truncate_chars(text: &str, limit: usize) -> String {
    let mut result = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= limit {
            break;
        }
        result.push(ch);
    }
    result
}

fn apply_width(text: String, width: Option<usize>, left_align: bool, zero_pad: bool) -> String {
    if let Some(width) = width {
        if left_align {
            format!("{text:<width$}")
        } else if zero_pad {
            format!("{text:0>width$}")
        } else {
            format!("{text:>width$}")
        }
    } else {
        text
    }
}

fn format_string_value(
    value: &TemplateValue,
    width: Option<usize>,
    precision: Option<usize>,
    left_align: bool,
) -> String {
    let mut text = value.as_string();
    if let Some(limit) = precision.filter(|&limit| text.chars().count() > limit) {
        text = truncate_chars(&text, limit);
    }
    apply_width(text, width, left_align, false)
}

fn format_int_value(
    value: &TemplateValue,
    width: Option<usize>,
    left_align: bool,
    zero_pad: bool,
) -> Result<String, String> {
    let number = value.as_i64()?;
    Ok(apply_width(
        number.to_string(),
        width,
        left_align,
        zero_pad && !left_align,
    ))
}

fn format_float_value(
    value: &TemplateValue,
    width: Option<usize>,
    precision: Option<usize>,
    left_align: bool,
    zero_pad: bool,
) -> Result<String, String> {
    let number = value.as_f64()?;
    let prec = precision.unwrap_or(6);
    let text = format!("{number:.prec$}");
    Ok(apply_width(
        text,
        width,
        left_align,
        zero_pad && !left_align,
    ))
}

struct StageContext<'a> {
    labels: BTreeMap<String, String>,
    extracted: BTreeMap<String, String>,
    line: String,
    original_line: String,
    original: &'a BTreeMap<String, String>,
    timestamp: DateTime<FixedOffset>,
    now: DateTime<FixedOffset>,
}

impl<'a> StageContext<'a> {
    fn new(labels: &'a BTreeMap<String, String>, line: String, timestamp_ns: i128) -> Self {
        let timestamp = ns_to_datetime(timestamp_ns).unwrap_or_else(epoch_fixed);
        let now_local = Local::now();
        let now = now_local.with_timezone(&now_local.offset().fix());
        Self {
            labels: labels.clone(),
            extracted: BTreeMap::new(),
            line: line.clone(),
            original_line: line,
            original: labels,
            timestamp,
            now,
        }
    }

    fn into_output(self) -> PipelineOutput {
        PipelineOutput {
            labels: self.labels,
            line: self.line,
        }
    }

    fn extract_logfmt(&mut self) {
        for (key, value) in parse_logfmt(&self.line) {
            self.insert_extracted(key, value);
        }
    }

    fn extract_json(&mut self, stage: &JsonStage) {
        let value: Value = match serde_json::from_str(&self.line) {
            Ok(value) => value,
            Err(_) => {
                self.labels
                    .entry("__error__".into())
                    .or_insert_with(|| "json_parser_error".into());
                return;
            }
        };
        match stage {
            JsonStage::All => flatten_json(&value, None, &mut |key, val| {
                self.insert_extracted(key, val)
            }),
            JsonStage::Selectors(selectors) => {
                for selector in selectors {
                    if let Some(selected) = selector.path.evaluate(&value)
                        && let Some(label) = sanitize_label(&selector.target)
                    {
                        let rendered = json_value_to_string(selected);
                        self.insert_extracted(label, rendered);
                    }
                }
            }
        }
    }

    fn drop_labels(&mut self, stage: &DropStage) {
        for target in &stage.targets {
            self.labels.remove(target);
            self.extracted.remove(target);
        }
    }

    fn apply_template(&mut self, template: &LineTemplate) {
        self.line = template.render(self);
    }

    fn format_labels(&mut self, stage: &LabelFormatStage) {
        for rule in &stage.rules {
            match &rule.value {
                LabelFormatValue::Template(template) => {
                    let value = template.render(self);
                    self.labels.insert(rule.target.clone(), value);
                }
                LabelFormatValue::Source(source) => {
                    if let Some(value) = self
                        .labels
                        .remove(source)
                        .or_else(|| self.extracted.get(source).cloned())
                    {
                        self.labels.insert(rule.target.clone(), value);
                    }
                }
            }
        }
    }

    fn insert_extracted(&mut self, key: String, value: String) {
        let target = if self.labels.contains_key(&key)
            || self.extracted.contains_key(&key)
            || self.original.contains_key(&key)
        {
            let fallback = format!("{}_extracted", key);
            if self.labels.contains_key(&fallback)
                || self.extracted.contains_key(&fallback)
                || self.original.contains_key(&fallback)
            {
                return;
            }
            fallback
        } else {
            key
        };
        self.extracted.entry(target).or_insert(value);
    }

    fn lookup(&self, name: &str) -> Option<&str> {
        self.extracted
            .get(name)
            .or_else(|| self.labels.get(name))
            .map(|s| s.as_str())
    }

    fn timestamp(&self) -> DateTime<FixedOffset> {
        self.timestamp
    }

    fn now(&self) -> DateTime<FixedOffset> {
        self.now
    }

    fn original_line(&self) -> &str {
        &self.original_line
    }
}

fn ns_to_datetime(ns: i128) -> Option<DateTime<FixedOffset>> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000);
    let utc = DateTime::<Utc>::from_timestamp(secs as i64, nanos as u32)?;
    let offset = FixedOffset::east_opt(0)?;
    Some(utc.with_timezone(&offset))
}

fn epoch_fixed() -> DateTime<FixedOffset> {
    let offset = FixedOffset::east_opt(0).unwrap();
    DateTime::<Utc>::from_timestamp(0, 0)
        .unwrap()
        .with_timezone(&offset)
}

fn parse_time_string(value: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(dt.offset()))
        .map_err(|_| format!("unable to parse `{value}` as timestamp"))
}

#[derive(Debug)]
struct ConvertedLayout {
    format: String,
    has_timezone: bool,
}

fn convert_go_layout(layout: &str) -> ConvertedLayout {
    const TOKENS: [(&str, &str, bool); 27] = [
        ("January", "%B", false),
        ("Monday", "%A", false),
        ("2006", "%Y", false),
        ("-07:00", "%:z", true),
        ("Z07:00", "%:z", true),
        ("-0700", "%z", true),
        ("Z0700", "%z", true),
        ("Jan", "%b", false),
        ("Mon", "%a", false),
        ("MST", "%Z", true),
        ("-07", "%z", true),
        ("Z07", "%z", true),
        ("15", "%H", false),
        ("03", "%I", false),
        ("05", "%S", false),
        ("04", "%M", false),
        ("02", "%d", false),
        ("06", "%y", false),
        ("01", "%m", false),
        ("_2", "%e", false),
        ("PM", "%p", false),
        ("pm", "%P", false),
        ("1", "%-m", false),
        ("2", "%-d", false),
        ("3", "%-I", false),
        ("4", "%-M", false),
        ("5", "%-S", false),
    ];
    let mut format = String::new();
    let mut has_timezone = false;
    let mut idx = 0;
    let chars = layout.as_bytes();
    while idx < chars.len() {
        if chars[idx] == b'.' {
            let mut zeros = 0;
            let mut look = idx + 1;
            while look < chars.len() && chars[look] == b'0' {
                zeros += 1;
                look += 1;
            }
            if zeros > 0 {
                let zeros = zeros.min(9);
                format.push_str(&format!("%.{zeros}f"));
                idx = look;
                continue;
            }
        }
        let mut matched = false;
        for (token, replacement, tz) in TOKENS {
            if layout[idx..].starts_with(token) {
                format.push_str(replacement);
                if tz {
                    has_timezone = true;
                }
                idx += token.len();
                matched = true;
                break;
            }
        }
        if matched {
            continue;
        }
        let ch = layout[idx..].chars().next().unwrap();
        if ch == '%' {
            format.push_str("%%");
        } else {
            format.push(ch);
        }
        idx += ch.len_utf8();
    }
    ConvertedLayout {
        format,
        has_timezone,
    }
}

fn format_with_layout(value: &DateTime<FixedOffset>, layout: &str) -> Result<String, String> {
    let converted = convert_go_layout(layout);
    Ok(value.format(&converted.format).to_string())
}

#[derive(Clone)]
enum ZoneSpec {
    Fixed(FixedOffset),
    Region(Tz),
}

fn parse_with_layout(
    layout: &str,
    input: &str,
    zone: Option<ZoneSpec>,
) -> Result<DateTime<FixedOffset>, String> {
    let converted = convert_go_layout(layout);
    if converted.has_timezone {
        let parsed = DateTime::parse_from_str(input, &converted.format)
            .map_err(|_| format!("unable to parse `{input}` with layout `{layout}`"))?;
        return Ok(parsed.with_timezone(parsed.offset()));
    }
    let naive = NaiveDateTime::parse_from_str(input, &converted.format)
        .map_err(|_| format!("unable to parse `{input}` with layout `{layout}`"))?;
    let zone = zone.unwrap_or_else(|| ZoneSpec::Fixed(local_offset()));
    match zone {
        ZoneSpec::Fixed(offset) => match offset.from_local_datetime(&naive) {
            LocalResult::Single(dt) => Ok(dt),
            LocalResult::Ambiguous(first, _) => Ok(first),
            LocalResult::None => Ok(offset.from_utc_datetime(&naive)),
        },
        ZoneSpec::Region(tz) => match tz.from_local_datetime(&naive) {
            LocalResult::Single(dt) => Ok(dt.with_timezone(&dt.offset().fix())),
            LocalResult::Ambiguous(first, _) => Ok(first.with_timezone(&first.offset().fix())),
            LocalResult::None => Err("timestamp does not exist in requested timezone".into()),
        },
    }
}

fn local_offset() -> FixedOffset {
    let now = Local::now();
    now.offset().fix()
}

fn parse_zone(name: &str) -> Result<ZoneSpec, String> {
    if name.eq_ignore_ascii_case("utc") {
        return Ok(ZoneSpec::Fixed(FixedOffset::east_opt(0).unwrap()));
    }
    if name.eq_ignore_ascii_case("local") {
        return Ok(ZoneSpec::Fixed(local_offset()));
    }
    match name.parse::<Tz>() {
        Ok(tz) => Ok(ZoneSpec::Region(tz)),
        Err(_) => Err(format!("unknown timezone `{name}`")),
    }
}

fn parse_logfmt(input: &str) -> BTreeMap<String, String> {
    let mut pairs = BTreeMap::new();
    let mut chars = input.chars().peekable();
    while skip_whitespace(&mut chars) {
        if let Some((key, value)) = parse_pair(&mut chars)
            && let Some(sanitized) = sanitize_label(&key)
        {
            pairs.entry(sanitized).or_insert(value);
        }
    }
    pairs
}

fn flatten_json<F>(value: &Value, prefix: Option<String>, insert: &mut F)
where
    F: FnMut(String, String),
{
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if let Some(sanitized) = sanitize_label(key) {
                    let next = match &prefix {
                        Some(existing) if !existing.is_empty() => {
                            format!("{}_{}", existing, sanitized)
                        }
                        Some(existing) => existing.clone(),
                        None => sanitized,
                    };
                    flatten_json(child, Some(next), insert);
                }
            }
        }
        Value::String(text) => {
            if let Some(name) = prefix {
                insert(name, text.clone());
            }
        }
        Value::Number(num) => {
            if let Some(name) = prefix {
                insert(name, num.to_string());
            }
        }
        Value::Bool(flag) => {
            if let Some(name) = prefix {
                insert(name, flag.to_string());
            }
        }
        Value::Array(_) | Value::Null => {}
    }
}

fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(num) => num.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn sanitize_label(name: &str) -> Option<String> {
    let mut result = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            result.push(ch);
        } else {
            result.push('_');
        }
    }
    if result.is_empty() {
        return None;
    }
    if result.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        result.insert(0, '_');
    }
    Some(result)
}

fn parse_pair<I>(chars: &mut std::iter::Peekable<I>) -> Option<(String, String)>
where
    I: Iterator<Item = char>,
{
    let key = parse_token(chars, |c| c == '=' || c.is_whitespace())?;
    match chars.peek() {
        Some('=') => {
            chars.next();
            let value = parse_value(chars);
            Some((key, value))
        }
        _ => Some((key, "true".into())),
    }
}

fn parse_value<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    match chars.peek() {
        Some('"') => parse_quoted(chars),
        _ => parse_token(chars, |c| c.is_whitespace()).unwrap_or_default(),
    }
}

fn parse_token<I, F>(chars: &mut std::iter::Peekable<I>, stop: F) -> Option<String>
where
    I: Iterator<Item = char>,
    F: Fn(char) -> bool,
{
    let mut token = String::new();
    while let Some(&ch) = chars.peek() {
        if stop(ch) {
            break;
        }
        token.push(ch);
        chars.next();
    }
    if token.is_empty() { None } else { Some(token) }
}

fn parse_quoted<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut result = String::new();
    chars.next();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => break,
            '\\' => {
                if let Some(next) = chars.next() {
                    result.push(next);
                }
            }
            _ => result.push(ch),
        }
    }
    result
}

fn skip_whitespace<I>(chars: &mut std::iter::Peekable<I>) -> bool
where
    I: Iterator<Item = char>,
{
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
    chars.peek().is_some()
}

fn find_subsequence(haystack: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    haystack[start..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| start + pos)
}

fn consume_ws(chars: &mut Peekable<Chars<'_>>) {
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_logfmt_pairs() {
        let result = parse_logfmt("method=GET status=200 duration=10ms message=\"hello world\"");
        assert_eq!(result.get("method"), Some(&"GET".to_string()));
        assert_eq!(result.get("status"), Some(&"200".to_string()));
        assert_eq!(result.get("duration"), Some(&"10ms".to_string()));
        assert_eq!(result.get("message"), Some(&"hello world".to_string()));
    }

    #[test]
    fn line_template_renders_segments() {
        let template = LineTemplate::compile("{{.status}} - {{.message}}".to_string()).unwrap();
        let labels = BTreeMap::new();
        let mut ctx = StageContext::new(&labels, String::new(), 0);
        ctx.extracted.insert("status".into(), "200".into());
        ctx.extracted.insert("message".into(), "ok".into());
        assert_eq!(template.render(&ctx), "200 - ok");
    }

    #[test]
    fn line_template_supports_functions() {
        let template = LineTemplate::compile(
            "{{ .status | default \"-\" }} {{ .message | upper | trim }}".to_string(),
        )
        .unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("message".into(), " hello ".into());
        let mut ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "- HELLO");
        ctx.extracted.insert("status".into(), "200".into());
        assert_eq!(template.render(&ctx), "200 HELLO");
    }

    #[test]
    fn line_template_supports_arguments() {
        let template =
            LineTemplate::compile("{{ substr 0 5 .message | repeat 2 }}".to_string()).unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("message".into(), "status=200".into());
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "statustatu");
    }

    #[test]
    fn line_template_supports_bytes() {
        let template = LineTemplate::compile("{{ bytes \"2 KB\" }}".to_string()).unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "2000");
    }

    #[test]
    fn line_template_supports_printf() {
        let template =
            LineTemplate::compile("{{ printf \"%5s-%02d\" .message 7 }}".to_string()).unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("message".into(), "go".into());
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "   go-07");
    }

    #[test]
    fn line_template_supports_from_json() {
        let template = LineTemplate::compile("{{ fromJson .payload }}".to_string()).unwrap();
        let mut labels = BTreeMap::new();
        labels.insert("payload".into(), "{\"foo\":1}".into());
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "{\"foo\":1}");
    }

    #[test]
    fn pipeline_processes_logfmt_then_template() {
        let template = LineTemplate::compile("{{.method}} {{.path}}".to_string()).unwrap();
        let pipeline = Pipeline::new(vec![
            PipelineStage::Logfmt,
            PipelineStage::LineFormat(template),
        ]);
        let labels = BTreeMap::new();
        let output = pipeline.process(&labels, "method=POST path=/ready status=200", 0);
        assert_eq!(output.line, "POST /ready");
    }

    #[test]
    fn json_stage_extracts_nested_fields() {
        let selector = JsonSelector {
            target: "duration".into(),
            path: JsonPath::new(vec![
                JsonPathSegment::Field("data".into()),
                JsonPathSegment::Field("latency".into()),
            ]),
        };
        let fmt = LineTemplate::compile("{{.duration}}".to_string()).unwrap();
        let pipeline = Pipeline::new(vec![
            PipelineStage::Json(JsonStage::Selectors(vec![selector])),
            PipelineStage::LineFormat(fmt),
        ]);
        let labels = BTreeMap::new();
        let output = pipeline.process(&labels, "{\"data\": {\"latency\": 123}}", 0);
        assert_eq!(output.line, "123");
    }

    #[test]
    fn label_format_renames_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("level".into(), "warn".into());
        let stage = LabelFormatStage {
            rules: vec![LabelFormatRule {
                target: "severity".into(),
                value: LabelFormatValue::Source("level".into()),
            }],
        };
        let pipeline = Pipeline::new(vec![PipelineStage::LabelFormat(stage)]);
        let output = pipeline.process(&labels, "line", 0);
        assert_eq!(output.labels.get("severity"), Some(&"warn".to_string()));
        assert!(!output.labels.contains_key("level"));
    }

    #[test]
    fn line_template_supports_unix_epoch() {
        let template =
            LineTemplate::compile("{{ __timestamp__ | unixEpoch }}".to_string()).unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, String::new(), 1_600_000_000_000_000_000);
        assert_eq!(template.render(&ctx), "1600000000");
    }

    #[test]
    fn line_template_supports_unix_to_time() {
        let template = LineTemplate::compile(
            "{{ unixToTime \"1679577215\" | date \"2006-01-02\" }}".to_string(),
        )
        .unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "2023-03-23");
    }

    #[test]
    fn line_template_supports_math_functions() {
        let template =
            LineTemplate::compile("{{ add 3 2 5 }} {{ round 123.5555 3 }}".to_string()).unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "10 123.556");
    }

    #[test]
    fn line_template_supports_regex_helpers() {
        let template = LineTemplate::compile("{{ __line__ | count \"foo\" }}".to_string()).unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, "foofoo".to_string(), 0);
        assert_eq!(template.render(&ctx), "2");
    }

    #[test]
    fn line_template_supports_to_date_in_zone() {
        let template = LineTemplate::compile(
            "{{ toDateInZone \"2006-01-02 15:04\" \"UTC\" \"2023-03-23 12:30\" | date \"2006-01-02T15:04\" }}"
                .to_string(),
        )
        .unwrap();
        let labels = BTreeMap::new();
        let ctx = StageContext::new(&labels, String::new(), 0);
        assert_eq!(template.render(&ctx), "2023-03-23T12:30");
    }

    #[test]
    fn parse_with_layout_supports_basic_case() {
        let zone = parse_zone("UTC").unwrap();
        let dt = parse_with_layout("2006-01-02 15:04", "2023-03-23 12:30", Some(zone)).unwrap();
        assert_eq!(dt.format("%Y-%m-%dT%H:%M").to_string(), "2023-03-23T12:30");
    }
}
