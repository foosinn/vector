use crate::{
    conditions::Condition,
    event::Event,
    topology::config::{TestCondition, TestDefinition},
    transforms::Transform,
};
use std::collections::HashMap;

//------------------------------------------------------------------------------

pub struct UnitTestCheck {
    extract_from: String,
    conditions: HashMap<String, Box<dyn Condition>>,
}

pub struct UnitTestTransform {
    transform: Box<dyn Transform>,
    next: Vec<String>,
}

pub struct UnitTest {
    name: String,
    input: (String, Event),
    transforms: HashMap<String, UnitTestTransform>,
    checks: Vec<UnitTestCheck>,
}

//------------------------------------------------------------------------------

fn walk(
    node: &str,
    inputs: Vec<Event>,
    transforms: &mut HashMap<String, UnitTestTransform>,
    aggregated_results: &mut HashMap<String, Vec<Event>>,
) {
    let mut results = Vec::new();
    let mut targets = Vec::new();

    if let Some(target) = transforms.get_mut(node) {
        for input in inputs {
            target.transform.transform_into(&mut results, input);
        }
        targets = target.next.clone();
    }

    for child in targets {
        walk(&child, results.clone(), transforms, aggregated_results);
    }
    aggregated_results.insert(node.into(), results);
}

impl UnitTest {
    pub fn run(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        let mut results = HashMap::new();

        walk(
            &self.input.0,
            vec![self.input.1.clone()],
            &mut self.transforms,
            &mut results,
        );

        for check in &self.checks {
            if let Some(results) = results.get(&check.extract_from) {
                for (name, cond) in &check.conditions {
                    if results.iter().find(|e| cond.check(e)).is_none() {
                        // TODO
                    }
                }
            } else {
                errors.push(format!(
                    "expected resulting events from transform '{}', found none",
                    check.extract_from
                ));
            }
        }

        errors
    }
}

//------------------------------------------------------------------------------

fn links_to_a_leaf(
    target: &str,
    leaves: &HashMap<String, ()>,
    link_checked: &mut HashMap<String, bool>,
    transform_outputs: &HashMap<String, HashMap<String, ()>>,
) -> bool {
    if let Some(check) = link_checked.get(target) {
        return *check;
    }
    let linked = leaves.contains_key(target)
        || if let Some(outputs) = transform_outputs.get(target) {
            outputs
                .iter()
                .filter(|(o, _)| links_to_a_leaf(o, leaves, link_checked, transform_outputs))
                .count()
                > 0
        } else {
            false
        };
    link_checked.insert(target.to_owned(), linked);
    linked
}

/// Reduces a collection of transforms into a set that only contains those that
/// link between our root (test input) and a set of leaves (test outputs).
fn reduce_transforms(
    root: &str,
    leaves: &HashMap<String, ()>,
    transform_outputs: &mut HashMap<String, HashMap<String, ()>>,
) {
    let mut link_checked: HashMap<String, bool> = HashMap::new();

    if !links_to_a_leaf(root, leaves, &mut link_checked, transform_outputs) {
        transform_outputs.clear();
    }

    transform_outputs.retain(|name, children| {
        let linked = name == root || *link_checked.get(name).unwrap_or(&false);
        if linked {
            // Also remove all unlinked children.
            children.retain(|child_name, _| {
                name == root || *link_checked.get(child_name).unwrap_or(&false)
            })
        }
        linked
    });
}

fn build_unit_test(
    definition: &TestDefinition,
    config: &super::Config,
) -> Result<UnitTest, Vec<String>> {
    let mut errors = vec![];

    // Maps transform names with their output targets (transforms that use it as
    // an input).
    let mut transform_outputs: HashMap<String, HashMap<String, ()>> = config
        .transforms
        .iter()
        .map(|(k, _)| (k.clone(), HashMap::new()))
        .collect();

    config.transforms.iter().for_each(|(k, t)| {
        t.inputs.iter().for_each(|i| {
            if let Some(outputs) = transform_outputs.get_mut(i) {
                outputs.insert(k.to_string(), ());
            }
        })
    });

    if !transform_outputs.contains_key(&definition.input.insert_at) {
        errors.push(format!(
            "unable to locate test target '{}'",
            definition.input.insert_at,
        ));
        return Err(errors);
    }

    let mut leaves: HashMap<String, ()> = HashMap::new();
    definition.outputs.iter().for_each(|o| {
        leaves.insert(o.extract_from.clone(), ());
    });

    // Reduce the configured transforms into just the ones connecting our test
    // target with output targets.
    reduce_transforms(&definition.input.insert_at, &leaves, &mut transform_outputs);

    // Build reduced transforms.
    let mut transforms: HashMap<String, UnitTestTransform> = HashMap::new();
    for (name, transform_config) in &config.transforms {
        if let Some(outputs) = transform_outputs.remove(name) {
            match transform_config.inner.build() {
                Ok(transform) => {
                    transforms.insert(
                        name.clone(),
                        UnitTestTransform {
                            transform: transform,
                            next: outputs.into_iter().map(|(k, _)| k).collect(),
                        },
                    );
                }
                Err(err) => {
                    errors.push(format!("failed to build transform '{}': {}", name, err));
                }
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    definition.outputs.iter().for_each(|o| {
        if !transform_outputs.contains_key(&o.extract_from) {
            errors.push(format!(
                "unable to complete topology between input target '{}' and '{}'",
                definition.input.insert_at, o.extract_from
            ));
        }
    });

    // Build input event.
    // TODO: Support different input event types.
    let input_event = match definition.input.type_str.as_ref() {
        "raw" => match definition.input.value.as_ref() {
            Some(v) => Event::from(v.clone()),
            None => {
                errors.push(format!("input type 'raw' requires the field 'value'"));
                Event::from("")
            }
        },
        _ => {
            errors.push(format!(
                "unrecognized input type '{}', expected one of: 'raw'",
                definition.input.type_str
            ));
            Event::from("")
        }
    };

    // Build all output conditions.
    let checks = definition.outputs.iter().map(|o| {
        let mut conditions: HashMap<String, Box<dyn Condition>> = HashMap::new();
        for (k, cond_conf) in &o.conditions {
            match cond_conf {
                TestCondition::Embedded(b) => {
                    match b.build() {
                        Ok(c) => {
                            // TODO c.init(foo)
                            conditions.insert(k.clone(), c);
                        },
                        Err(e) => {
                            errors.push(format!(
                                "failed to create test condition '{}': {}",
                                k, e,
                            ));
                        },
                    }
                },
                TestCondition::String(_s) => {
                    errors.push(format!("failed to create test condition '{}': string conditions are not yet supported", k));
                },
            }
        }
        UnitTestCheck{
            extract_from: o.extract_from.clone(),
            conditions: conditions,
        }
    }).collect();

    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(UnitTest {
            name: definition.name.clone(),
            input: (definition.input.insert_at.clone(), input_event),
            transforms: transforms,
            checks: checks,
        })
    }
}

pub fn build_unit_tests(config: &super::Config) -> Result<Vec<UnitTest>, Vec<String>> {
    let mut tests = vec![];
    let mut errors = vec![];

    config
        .tests
        .iter()
        .for_each(|test| match build_unit_test(test, config) {
            Ok(t) => tests.push(t),
            Err(errs) => {
                let mut test_err = errs.join("\n");
                // Indent all line breaks
                test_err = test_err.replace("\n", "\n\t");
                test_err.insert_str(0, &format!("Failed to build test '{}':\n", test.name));
                errors.push(test_err);
            }
        });

    if errors.is_empty() {
        Ok(tests)
    } else {
        Err(errors)
    }
}
