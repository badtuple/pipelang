use errors::Error;
use filters::Filter;
use std::collections::HashMap;

mod errors;
mod filters;
mod parser;

#[derive(Default)]
pub struct Interpreter {
    // sources is a map of Source name to Source
    sources: HashMap<String, Source>,

    // pipelines is a map of Source name to Pipeline
    pipelines: HashMap<String, Pipeline>,

    // registery of filter names to filter objects
    filter_registry: HashMap<String, Box<dyn Filter>>,
}

impl Interpreter {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            pipelines: HashMap::new(),
            filter_registry: HashMap::new(),
        }
    }

    pub fn exec(&mut self, query: String) -> Result<(), Error> {
        let pipeline = parser::parse_pipeline(&self.filter_registry, query)?;
        self.register_source(Source::new(pipeline.source_name.clone()));
        self.register_pipeline(pipeline);
        Ok(())
    }

    pub fn register_filter(&mut self, name: String, filter: Box<dyn Filter>) {
        self.filter_registry.insert(name, filter);
    }

    fn register_source(&mut self, source: Source) {
        self.sources.insert(source.name.clone(), source);
    }

    fn register_pipeline(&mut self, pipeline: Pipeline) {
        self.pipelines
            .insert(pipeline.source_name.clone(), pipeline);
    }

    pub fn push_data_to_source(&mut self, name: String, mut data: Vec<Datum>) -> Result<(), Error> {
        match self.sources.get_mut(&name) {
            Some(source) => source.data.append(&mut data),
            None => return Err(Error::CannotPushToUnregisteredSource),
        };

        Ok(())
    }

    pub fn process_source(&mut self, source_name: String) -> Result<Vec<Option<Datum>>, Error> {
        let pipeline = match self.pipelines.get_mut(&source_name) {
            Some(pipeline) => pipeline,
            None => return Err(Error::CannotReadFromUnregisteredSource),
        };

        let data: Vec<Datum> = match self.sources.get_mut(&source_name) {
            Some(source) => source.data.drain(..).collect(),
            None => return Err(Error::CannotReadFromUnregisteredSource),
        };

        let out = data
            .iter()
            .map(|datum| {
                // Pass data through Filters
                let mut curr_datum: Option<Datum> = Some(datum.clone());
                for filter in pipeline.filters.iter_mut() {
                    curr_datum = filter.exec(curr_datum?).unwrap();
                }
                curr_datum
            })
            .filter(|d| d.is_some())
            .collect();

        Ok(out)
    }
}

struct Source {
    name: String,
    data: Vec<Datum>,
}

impl Source {
    fn new(name: String) -> Self {
        Source { name, data: vec![] }
    }
}

pub struct Pipeline {
    source_name: String,
    filters: Vec<Box<dyn Filter>>,
}

impl Pipeline {
    pub fn new(source_name: String, filters: Vec<Box<dyn Filter>>) -> Self {
        Pipeline {
            source_name,
            filters,
        }
    }
}

// A single piece of data from a Source that will be fed through the Pipeline.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Datum {
    Integer(i64),
    Vec(Vec<Datum>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Error;
    use crate::filters::{Batch, GreaterThan};

    #[test]
    fn can_run_empty_pipeline() {
        let mut interpreter = Interpreter::new();

        let source = Source::new("sensor".into());
        interpreter.register_source(source);

        let data = vec![Datum::Integer(4)];
        interpreter
            .push_data_to_source("sensor".into(), data)
            .expect("could not push to interpreter");

        let pipeline = Pipeline::new("sensor".into(), vec![]);
        interpreter.register_pipeline(pipeline);

        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum::Integer(4)));
    }

    #[test]
    fn can_run_pipeline_with_filters() {
        #[derive(Clone)]
        struct Double;
        impl Filter for Double {
            fn exec(&mut self, datum: Datum) -> Result<Option<Datum>, Error> {
                match datum {
                    Datum::Integer(i) => Ok(Some(Datum::Integer(i * 2))),
                    _ => Err(Error::FilterCannotProcessDataType),
                }
            }
        }

        let mut interpreter = Interpreter::new();
        let source = Source::new("sensor".into());
        interpreter.register_source(source);

        let data = vec![Datum::Integer(4)];
        interpreter
            .push_data_to_source("sensor".into(), data)
            .expect("could not push to interpreter");

        let pipeline = Pipeline::new("sensor".into(), vec![Box::new(Double), Box::new(Double)]);
        interpreter.register_pipeline(pipeline);

        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum::Integer(16)));
    }

    #[test]
    fn can_handle_none_returns_from_filter() {
        let mut interpreter = Interpreter::new();
        let source = Source::new("sensor".into());
        interpreter.register_source(source);

        let data = vec![Datum::Integer(3), Datum::Integer(42)];
        interpreter
            .push_data_to_source("sensor".into(), data)
            .expect("could not push to interpreter");

        let pipeline = Pipeline::new("sensor".into(), vec![Box::new(GreaterThan { op: 12 })]);
        interpreter.register_pipeline(pipeline);

        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum::Integer(42)));
    }

    #[test]
    fn can_handle_aggregate_filters() {
        let mut interpreter = Interpreter::new();
        let source = Source::new("sensor".into());
        interpreter.register_source(source);

        let data = vec![Datum::Integer(1), Datum::Integer(2), Datum::Integer(3)];
        interpreter
            .push_data_to_source("sensor".into(), data.clone())
            .expect("could not push to interpreter");

        let pipeline = Pipeline::new(
            "sensor".into(),
            vec![Box::new(Batch {
                n: 2,
                in_progress: vec![],
            })],
        );
        interpreter.register_pipeline(pipeline);

        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");
        assert_eq!(data_out.len(), 1);
        assert_eq!(
            data_out[0],
            Some(Datum::Vec(vec!(Datum::Integer(1), Datum::Integer(2))))
        );

        interpreter
            .push_data_to_source("sensor".into(), data.clone())
            .expect("could not push to interpreter");
        let pipeline = Pipeline::new(
            "sensor".into(),
            vec![Box::new(Batch {
                n: 1,
                in_progress: vec![],
            })],
        );
        interpreter.register_pipeline(pipeline);
        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");
        assert_eq!(data_out.len(), 3);
        assert_eq!(
            data_out,
            vec!(
                Some(Datum::Vec(vec!(Datum::Integer(1)))),
                Some(Datum::Vec(vec!(Datum::Integer(2)))),
                Some(Datum::Vec(vec!(Datum::Integer(3)))),
            )
        );

        interpreter
            .push_data_to_source("sensor".into(), data.clone())
            .expect("could not push to interpreter");
        let pipeline = Pipeline::new(
            "sensor".into(),
            vec![Box::new(Batch {
                n: 5,
                in_progress: vec![],
            })],
        );
        interpreter.register_pipeline(pipeline);

        let data_out = interpreter
            .process_source("sensor".into())
            .expect("error returned from pipeline");

        assert_eq!(data_out.len(), 0);
    }
}
