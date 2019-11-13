use crate::{
    conditions::Condition, event::Event, topology::config::TestDefinition, transforms::Transform,
};
use std::collections::HashMap;

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

    let mut prev_inputs_len = inputs.len();

    let mut transforms: Vec<(String, Vec<String>, Box<dyn Transform>)> = config
        .transforms
        .iter()
        .filter_map(|(k, t)| {
            let transform_inputs: Vec<String> = t
                .inputs
                .iter()
                .map(|i| i.clone())
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
        prev_inputs_len = inputs.len(); // TODO
    }

    Err(errors)
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
