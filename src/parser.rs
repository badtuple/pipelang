use crate::{errors::Error, Filter, Pipeline};
use std::collections::HashMap;

pub fn parse_pipeline(
    filter_registry: &HashMap<String, Box<dyn Filter>>,
    query: String,
) -> Result<Pipeline, Error> {
    let mut lexer = Lexer::new(query);
    let tokens = lexer.tokenize()?;

    if tokens.is_empty() {
        return Err(Error::MalformedQuery(parse_err::EMPTY_QUERY.into()));
    }

    let source_name = match &tokens[0] {
        Token::Source(s) => s.clone(),
        _ => {
            return Err(Error::MalformedQuery(
                parse_err::DOES_NOT_START_WITH_SOURCE.into(),
            ))
        }
    };

    let mut filters: Vec<Box<dyn Filter>> = vec![];

    if tokens.len() > 1 {
        // TODO: Right now Pipelines don't have a way to accept Args so we
        // aren't parsing them. This also allows us to skip the rest of the
        // tokens right now.

        for token in &tokens[1..] {
            match token {
                Token::Source(_) => {
                    return Err(Error::MalformedQuery(parse_err::MULTIPLE_SOURCES.into()))
                }
                Token::Filter(name) => match filter_registry.get(name) {
                    Some(filter) => {
                        filters.push(filter.clone());
                    }
                    None => {
                        return Err(Error::MalformedQuery(parse_err::UNREGISTERED_FILTER.into()))
                    }
                },
                Token::OpenParen
                | Token::CloseParen
                | Token::String(_)
                | Token::Integer(_)
                | Token::Float(_) => continue,
            }
        }
    }

    Ok(Pipeline::new(source_name, filters))
}

struct Lexer {
    query: Vec<char>,
    cursor: usize,
}

impl Lexer {
    fn new(query: String) -> Self {
        Lexer {
            query: query.chars().collect(),
            cursor: 0,
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, Error> {
        let mut tokens = vec![];

        while let Some(c) = self.next_char() {
            if c.is_whitespace() {
                continue;
            }

            let token = match c {
                '@' => self.consume_source()?,
                '|' => self.consume_filter()?,
                '(' => Token::OpenParen,
                ')' => Token::CloseParen,
                '"' => self.consume_string_literal()?,
                '0'..='9' => {
                    self.rewind_cursor();
                    self.consume_number_literal()?
                }
                _ => panic!("unexpected token {:?}", c),
            };

            tokens.push(token);
        }

        Ok(tokens)
    }

    fn next_char(&mut self) -> Option<&char> {
        let c = self.query.get(self.cursor);
        self.cursor += 1;
        c
    }

    fn rewind_cursor(&mut self) {
        self.cursor -= 1;
    }

    fn consume_number_literal(&mut self) -> Result<Token, Error> {
        let mut buf = String::new();
        let mut has_decimal = false;

        loop {
            let c = match self.next_char() {
                Some(c) => c,
                None => {
                    self.rewind_cursor();
                    break;
                }
            };

            if !c.is_numeric() && *c != '.' {
                self.rewind_cursor();
                break;
            }

            if *c == '.' {
                if has_decimal {
                    return Err(Error::MalformedQuery(
                        lex_err::TOO_MANY_DECIMAL_POINTS.into(),
                    ));
                }

                has_decimal = true
            }

            buf.push(*c);
        }

        if has_decimal {
            Ok(Token::Float(
                buf.parse().expect("tried to parse non-float as a float"),
            ))
        } else {
            Ok(Token::Integer(
                buf.parse().expect("tried to parse non-float as a float"),
            ))
        }
    }

    fn consume_string_literal(&mut self) -> Result<Token, Error> {
        let mut buf = String::new();

        loop {
            let c = match self.next_char() {
                Some(c) => c,
                None => return Err(Error::MalformedQuery(lex_err::UNTERMINATED_STRING.into())),
            };

            if *c == '"' {
                break;
            }

            buf.push(*c);
        }

        Ok(Token::String(buf))
    }

    fn consume_source(&mut self) -> Result<Token, Error> {
        let mut source_name = String::new();

        loop {
            let c = match self.next_char() {
                Some(c) => c,
                None => {
                    self.rewind_cursor();
                    break;
                }
            };

            if !c.is_alphanumeric() && *c != '_' {
                self.rewind_cursor();
                break;
            }

            source_name.push(*c);
        }

        Ok(Token::Source(source_name))
    }

    fn consume_filter(&mut self) -> Result<Token, Error> {
        let mut filter_name = String::new();

        let mut has_begun_identifier = false;
        loop {
            let c = match self.next_char() {
                Some(c) => c,
                None => {
                    self.rewind_cursor();
                    break;
                }
            };

            if c.is_whitespace() && !has_begun_identifier {
                continue;
            }

            if c.is_whitespace() && has_begun_identifier {
                break;
            }

            if !c.is_alphanumeric() && *c != '_' {
                self.rewind_cursor();
                break;
            }

            has_begun_identifier = true;
            filter_name.push(*c);
        }

        Ok(Token::Filter(filter_name))
    }
}

#[derive(PartialEq, Debug)]
enum Token {
    Source(String),
    Filter(String),
    OpenParen,
    CloseParen,
    String(String),
    Integer(i64),
    Float(f64),
}

mod lex_err {
    pub const UNTERMINATED_STRING: &str = "query contained unterminated string";
    pub const TOO_MANY_DECIMAL_POINTS: &str = "float literals can only contain one decimal point";
}

mod parse_err {
    pub const EMPTY_QUERY: &str = "query cannot be empty";
    pub const DOES_NOT_START_WITH_SOURCE: &str = "query must start with source";
    pub const MULTIPLE_SOURCES: &str = "cannot have multiple sources";
    pub const UNREGISTERED_FILTER: &str = "referenced an unregistered filter";
}

#[cfg(test)]

mod test {
    use super::*;

    #[test]
    fn lexer_can_lex_sources() {
        let mut lex = Lexer::new("@sensor".into());
        let tokens = lex.tokenize().expect("lexer could not recognize source");
        assert_eq!(tokens, vec!(Token::Source("sensor".into())));
    }

    #[test]
    fn lexer_can_lex_filters() {
        let query = "@sensor | isNotZero | double".into();
        let mut lex = Lexer::new(query);
        let tokens = lex.tokenize().expect("lexer could not recognize filters");

        assert_eq!(
            tokens,
            vec!(
                Token::Source("sensor".into()),
                Token::Filter("isNotZero".into()),
                Token::Filter("double".into())
            )
        );
    }

    #[test]
    fn lexer_can_parse_string_literals() {
        let query = "@sensor | contains(\"signal\") | double".into();
        let mut lex = Lexer::new(query);
        let tokens = lex
            .tokenize()
            .expect("lexer could not parse string literal");

        assert_eq!(
            tokens,
            vec!(
                Token::Source("sensor".into()),
                Token::Filter("contains".into()),
                Token::OpenParen,
                Token::String("signal".into()),
                Token::CloseParen,
                Token::Filter("double".into())
            )
        );
    }

    #[test]
    fn lexer_can_parse_integer_literals() {
        let query = "@sensor | greater_than(4) | double".into();
        let mut lex = Lexer::new(query);
        let tokens = lex
            .tokenize()
            .expect("lexer could not parse string literal");

        assert_eq!(
            tokens,
            vec!(
                Token::Source("sensor".into()),
                Token::Filter("greater_than".into()),
                Token::OpenParen,
                Token::Integer(4),
                Token::CloseParen,
                Token::Filter("double".into())
            )
        );
    }

    #[test]
    fn lexer_can_parse_float_literals() {
        let query = "@sensor | greater_than(4.4) | double".into();
        let mut lex = Lexer::new(query);
        let tokens = lex
            .tokenize()
            .expect("lexer could not parse string literal");

        assert_eq!(
            tokens,
            vec!(
                Token::Source("sensor".into()),
                Token::Filter("greater_than".into()),
                Token::OpenParen,
                Token::Float(4.4),
                Token::CloseParen,
                Token::Filter("double".into())
            )
        );
    }
}
