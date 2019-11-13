use crate::topology::config::component::ComponentDescription;
use crate::Event;
use indexmap::IndexMap;
use inventory;
use std::sync::Arc;

pub mod check_fields;
pub mod not;
pub mod static_value;

pub trait Condition: Send + Sync {
    /// Before checking a condition against events it needs to be initialized by
    /// providing a vec of all sibling conditions. This allows the condition to
    /// aggregate any children when applicable.
    fn init(&mut self, siblings: &IndexMap<String, Arc<dyn Condition>>) -> crate::Result<()>;

    fn check(&self, e: &Event) -> bool; // TODO: Add method that provides fail context? -> Result<(), String>
}

#[typetag::serde(tag = "type")]
pub trait ConditionConfig: std::fmt::Debug {
    fn build(&self) -> crate::Result<Box<dyn Condition>>;
}

pub type ConditionDescription = ComponentDescription<Box<dyn ConditionConfig>>;

inventory::collect!(ConditionDescription);
