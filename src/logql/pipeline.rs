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

use std::{collections::BTreeMap, iter::Peekable, str::Chars};

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

    pub fn process(&self, labels: &BTreeMap<String, String>, line: &str) -> PipelineOutput {
        if self.stages.is_empty() {
            return PipelineOutput {
                labels: labels.clone(),
                line: line.to_string(),
            };
        }
        let mut ctx = StageContext::new(labels, line.to_string());
        for stage in &self.stages {
            stage.apply(&mut ctx);
        }
        ctx.into_output()
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
            if !token.starts_with('.') {
                return Err("line_format placeholder must start with `.`".into());
            }
            let field = token.trim_start_matches('.').to_string();
            if field.is_empty() {
                return Err("line_format placeholder must have a field name".into());
            }
            segments.push(TemplateSegment::Field(field));
            start = close + 2;
        }
        if start < source.len() {
            segments.push(TemplateSegment::Literal(source[start..].to_string()));
        }
        Ok(Self { segments })
    }

    fn render(&self, ctx: &StageContext<'_>) -> String {
        let mut result = String::new();
        for segment in &self.segments {
            match segment {
                TemplateSegment::Literal(text) => result.push_str(text),
                TemplateSegment::Field(name) => {
                    result.push_str(ctx.lookup(name).unwrap_or(""));
                }
            }
        }
        result
    }
}

#[derive(Debug, Clone)]
enum TemplateSegment {
    Literal(String),
    Field(String),
}

struct StageContext<'a> {
    labels: BTreeMap<String, String>,
    extracted: BTreeMap<String, String>,
    line: String,
    original: &'a BTreeMap<String, String>,
}

impl<'a> StageContext<'a> {
    fn new(labels: &'a BTreeMap<String, String>, line: String) -> Self {
        Self {
            labels: labels.clone(),
            extracted: BTreeMap::new(),
            line,
            original: labels,
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
        let mut ctx = StageContext::new(&labels, String::new());
        ctx.extracted.insert("status".into(), "200".into());
        ctx.extracted.insert("message".into(), "ok".into());
        assert_eq!(template.render(&ctx), "200 - ok");
    }

    #[test]
    fn pipeline_processes_logfmt_then_template() {
        let template = LineTemplate::compile("{{.method}} {{.path}}".to_string()).unwrap();
        let pipeline = Pipeline::new(vec![
            PipelineStage::Logfmt,
            PipelineStage::LineFormat(template),
        ]);
        let labels = BTreeMap::new();
        let output = pipeline.process(&labels, "method=POST path=/ready status=200");
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
        let output = pipeline.process(&labels, "{\"data\": {\"latency\": 123}}");
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
        let output = pipeline.process(&labels, "line");
        assert_eq!(output.labels.get("severity"), Some(&"warn".to_string()));
        assert!(!output.labels.contains_key("level"));
    }
}
