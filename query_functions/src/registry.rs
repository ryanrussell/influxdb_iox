use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::{DataFusionError, Result as DataFusionResult},
    logical_expr::{AggregateUDF, ScalarUDF},
    logical_plan::FunctionRegistry,
};

use crate::window;

lazy_static::lazy_static! {
    static ref REGISTRY: IOxFunctionRegistry =  IOxFunctionRegistry::new();
}

/// Lookup for all DataFusion User Defined Functions used by IOx
#[derive(Debug)]
pub(crate) struct IOxFunctionRegistry {}

impl IOxFunctionRegistry {
    fn new() -> Self {
        Self {}
    }
}

impl FunctionRegistry for IOxFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        [
            iox_regex::REGEX_MATCH_UDF_NAME,
            iox_regex::REGEX_NOT_MATCH_UDF_NAME,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn udf(&self, name: &str) -> DataFusionResult<Arc<ScalarUDF>> {
        match name {
            iox_regex::REGEX_MATCH_UDF_NAME => Ok(iox_regex::REGEX_MATCH_UDF.clone()),
            iox_regex::REGEX_NOT_MATCH_UDF_NAME => Ok(iox_regex::REGEX_NOT_MATCH_UDF.clone()),
            window::WINDOW_BOUNDS_UDF_NAME => Ok(window::WINDOW_BOUNDS_UDF.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "IOx FunctionRegistry does not contain function '{}'",
                name
            ))),
        }
    }

    fn udaf(&self, name: &str) -> DataFusionResult<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(format!(
            "IOx FunctionRegistry does not contain user defined aggregate function '{}'",
            name
        )))
    }
}

/// Return a reference to the global function registry
pub(crate) fn instance() -> &'static IOxFunctionRegistry {
    &REGISTRY
}
