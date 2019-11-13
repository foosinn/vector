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

impl UnitTest {
    pub fn run(&self) -> Result<(), Vec<String>> {
        // TODO
        Ok(())
    }
}

//------------------------------------------------------------------------------

fn build_unit_test(
    definition: &TestDefinition,
    config: &super::Config,
) -> Result<UnitTest, Vec<String>> {
    let mut errors = vec![];

    let mut inputs: HashMap<String, ()> = config
        .transforms
        .iter()
        .map(|(k, _)| (k.clone(), ()))
        .collect();

    if !inputs.contains_key(&definition.input.insert_at) {
        errors.push(format!(
            "unable to locate test target '{}'",
            definition.input.insert_at,
        ));
        return Err(errors);
    }

    let mut prev_inputs_len = inputs.len();

    let mut transforms: Vec<(String, Vec<String>, Box<dyn Transform>)> = config
        .transforms
        .iter()
        .filter_map(|(k, t)| {
            let transform_inputs: Vec<String> = t
                .inputs
                .iter()
                .cloned()
                .filter(|i| inputs.contains_key(i))
                .collect();

            if transform_inputs.len() == 0 && *k != definition.input.insert_at {
                inputs.remove(k);
                return None;
            }

            match t.inner.build() {
                Ok(transform) => Some((k.clone(), transform_inputs, transform)),
                Err(err) => {
                    errors.push(format!("failed to build transform '{}': {}", k, err));
                    None
                }
            }
        })
        .collect();

    if !errors.is_empty() {
        return Err(errors);
    }

    // Keep reducing our transforms until we have the smallest possible set.
    while prev_inputs_len > inputs.len() {
        prev_inputs_len = inputs.len();

        transforms = transforms
            .into_iter()
            .filter_map(|(k, t_inputs, transform)| {
                let transform_inputs: Vec<String> = t_inputs
                    .into_iter()
                    .filter(|i| inputs.contains_key(i))
                    .collect();

                if transform_inputs.len() == 0 && k != definition.input.insert_at {
                    inputs.remove(&k);
                    return None;
                }

                Some((k.clone(), transform_inputs, transform))
            })
            .collect();
    }

    definition.outputs.iter().for_each(|o| {
        if !inputs.contains_key(&o.extract_from) {
            errors.push(format!(
                "unable to complete topology between input target '{}' and '{}'",
                definition.input.insert_at, o.extract_from
            ));
        }
    });

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
            transforms: HashMap::new(), // TODO HashMap<String, UnitTestTransform>,
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
            Err(errs) => errors.extend(errs),
        });

    if errors.is_empty() {
        Ok(tests)
    } else {
        Err(errors)
    }
}
