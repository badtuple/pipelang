mod errors;
mod filters;

pub struct Interpreter;

impl Interpreter {
    pub fn new() -> Self {
        Self {}
    }

    //pub fn exec(&self, _query: String) {
    // TODO: Parse query into AST

    // TODO: AST to runnable Pipeline

    // Execute the pipeline on the data
    //let pipeline = Pipeline::new(vec![Datum { data: 4 }], vec![]);
    //self.exec_pipeline(pipeline);
    //}

    fn exec_pipeline(&self, mut pipeline: Pipeline) -> Vec<Option<Datum>> {
        // TODO: Right now we're collecting all the values upfront, which isn't the best call for
        // long running pipelines. Eventually we'll need to pass results up to the user as they
        // finish running through the pipeline;

        // TODO: This means state isn't persisted across runs
        let mut filters = pipeline.filters;

        pipeline
            .data
            .iter_mut()
            .map(|datum| {
                // Pass data through Filters
                let mut curr_datum: Option<Datum> = Some(datum.clone());
                for filter in filters.iter_mut() {
                    curr_datum = filter.exec(curr_datum?).unwrap();
                }
                curr_datum
            })
            .filter(|d| d.is_some())
            .collect()
    }
}

struct Pipeline {
    data: Vec<Datum>,
    filters: Vec<Box<dyn Filter>>,
}

impl Pipeline {
    fn new(data: Vec<Datum>, filters: Vec<Box<dyn Filter>>) -> Self {
        Pipeline { data, filters }
    }
}

pub trait Filter {
    fn exec(&mut self, datum: Datum) -> Result<Option<Datum>, errors::Error>;
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
        let data = vec![Datum::Integer(4)];
        let pipeline = Pipeline::new(data, vec![]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
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

        let data = vec![Datum::Integer(4)];
        let pipeline = Pipeline::new(data, vec![Box::new(Double), Box::new(Double)]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum::Integer(16)));
    }

    #[test]
    fn can_handle_none_returns_from_filter() {
        let data = vec![Datum::Integer(3), Datum::Integer(42)];
        let pipeline = Pipeline::new(data, vec![Box::new(GreaterThan { op: 12 })]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum::Integer(42)));
    }

    #[test]
    fn can_handle_aggregate_filters() {
        let interpreter = Interpreter::new();

        let data = vec![Datum::Integer(1), Datum::Integer(2), Datum::Integer(3)];
        let pipeline = Pipeline::new(
            data.clone(),
            vec![Box::new(Batch {
                n: 2,
                in_progress: vec![],
            })],
        );

        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(
            data_out[0],
            Some(Datum::Vec(vec!(Datum::Integer(1), Datum::Integer(2))))
        );

        let pipeline = Pipeline::new(
            data.clone(),
            vec![Box::new(Batch {
                n: 1,
                in_progress: vec![],
            })],
        );
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 3);
        assert_eq!(
            data_out,
            vec!(
                Some(Datum::Vec(vec!(Datum::Integer(1)))),
                Some(Datum::Vec(vec!(Datum::Integer(2)))),
                Some(Datum::Vec(vec!(Datum::Integer(3)))),
            )
        );

        let pipeline = Pipeline::new(
            data.clone(),
            vec![Box::new(Batch {
                n: 5,
                in_progress: vec![],
            })],
        );

        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 0);
    }
}
