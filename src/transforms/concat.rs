use super::Transform;
use crate::{
	event::{Event, ValueKind},
	topology::config::{DataType, TransformConfig, TransformDescription},
};
use serde::{Deserialize, Serialize};
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
			.collect::<Result<Vec<Substring>, &'static str>>()?;
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
	start: Option<u8>,
	end: Option<u8>,
}

impl Substring {
	pub fn new(input: &Atom) -> Result<Substring, &'static str> {
		let mut source = String::from("");
		let mut start: Option<u8> = None;
		let mut end: Option<u8> = None;
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
								start = match buffer.parse() {
									Ok(val) => Some(val),
									Err(_) => None,
								};
								buffer = String::from("");
								it.next();
								assert!(
									'.' == *(it
										.peek()
										.unwrap()),
									"invalid format, use [start..end]"
								);
								it.next();
							}
							'0'...'9' => {
								buffer.push(c);
								it.next();
							}
							']' => {
								end = match buffer.parse() {
									Ok(val) => Some(val),
									Err(_) => None,
								};
								return Ok(Self {
									source: Atom::from(source),
									start: start,
									end: end,
								});
							}
							_ => return Err(
								"invalid format, missing ']'",
							),
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
		Err("invalid format")
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
		//.concat();
		event.as_mut_log()
			.insert_explicit(Atom::from(self.target.clone()), ValueKind::from(value));
		event.as_log()
			.all_fields()
			.for_each(|(k, v)| println!("{} {:?}", k, v));
		Some(event)
	}
}
