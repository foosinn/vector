use super::{BuildError, Transform};
use crate::{
    event::{Event, ValueKind},
    topology::config::{DataType, TransformConfig, TransformDescription},
};
use serde::{Deserialize, Serialize};
use std::iter::Peekable;
use string_cache::DefaultAtom as Atom;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConcatConfig {
    pub target: String,
    pub joiner: String,
    pub items: Vec<Atom>,
}

inventory::submit! {
    TransformDescription::new_without_default::<ConcatConfig>("concat")
}

#[typetag::serde(name = "concat")]
impl TransformConfig for ConcatConfig {
    fn build(&self) -> crate::Result<Box<dyn Transform>> {
        let items_with_err = self
            .items
            .iter()
            .map(|item| Substring::new(item))
            .collect::<Result<Vec<Substring>, BuildError>>()?;
        Ok(Box::new(Concat::new(
            self.target.clone(),
            self.joiner.clone(),
            items_with_err,
        )))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn transform_type(&self) -> &'static str {
        "concat"
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Substring {
    source: Atom,
    start: Option<usize>,
    end: Option<usize>,
}

impl Substring {
    fn new(input: &Atom) -> Result<Substring, BuildError> {
        let mut source = String::from("");
        let mut start: Option<usize> = None;
        let mut end: Option<usize> = None;
        let mut buffer = String::from("");
        let mut it = input.chars().peekable();
        while let Some(&c) = it.peek() {
            match c {
                '[' => {
                    source = buffer;
                    buffer = String::from("");
                    it.next();
                    while let Some(&c) = it.peek() {
                        match c {
                            '.' => {
                                start = match buffer.parse::<u8>() {
                                    Ok(val) => Some(val as usize),
                                    Err(_) => None,
                                };
                                buffer = String::from("");
                                it.next();
                                if *(it.peek().unwrap()) != '.' {
                                    return Err(BuildError::InvalidSubstring {
                                        name: "invalid format, use source[start..end]".to_string(),
                                    });
                                }
                                it.next();
                            }
                            '0'..='9' => {
                                buffer.push(c);
                                it.next();
                            }
                            ']' => {
                                end = match buffer.parse::<u8>() {
                                    Ok(val) => Some(val as usize),
                                    Err(_) => None,
                                };
                                return Ok(Self {
                                    source: Atom::from(source),
                                    start: start,
                                    end: end,
                                });
                            }
                            _ => {
                                return Err(BuildError::InvalidSubstring {
                                    name: "invalid format, missing ']'".to_string(),
                                })
                            }
                        }
                    }
                }
                _ => {
                    it.next();
                    buffer.push(c)
                }
            }
        }
        if buffer.len() > 0 {
            return Ok(Self {
                source: Atom::from(buffer),
                start: None,
                end: None,
            });
        }
        Err(BuildError::InvalidSubstring {
            name: "invalid format, use source[start..end]".to_string(),
        })
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Concat {
    target: String,
    joiner: String,
    items: Vec<Substring>,
}

impl Concat {
    pub fn new(target: String, joiner: String, items: Vec<Substring>) -> Self {
        Self {
            target,
            joiner,
            items,
        }
    }
}

impl Transform for Concat {
    fn transform(&mut self, mut event: Event) -> Option<Event> {
        let value = self
            .items
            .iter()
            .filter_map(|substring| {
                if let Some(value) = event.as_log().get(&substring.source) {
                    let b = value.as_bytes();
                    let start = match substring.start {
                        Some(s) => s as usize,
                        None => 0,
                    };
                    let end = match substring.end {
                        Some(e) => e as usize,
                        None => b.len(),
                    };
                    return Some(b.slice(start, end));
                }
                None
            })
            .collect::<Vec<bytes::Bytes>>()
            .join(self.joiner.as_bytes());
        event
            .as_mut_log()
            .insert_explicit(Atom::from(self.target.clone()), ValueKind::from(value));
        event
            .as_log()
            .all_fields()
            .for_each(|(k, v)| println!("{} {:?}", k, v));
        Some(event)
    }
}
