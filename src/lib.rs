pub struct Interpreter;

impl Interpreter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn exec(&self, query: String) {
        // TODO: Parse query into AST

        // TODO: AST to runnable Pipeline

        // Execute the pipeline on the data
        let pipeline = Pipeline::new(vec![Datum { data: 4 }], vec![]);
        self.exec_pipeline(pipeline);
    }

    fn exec_pipeline(&self, pipeline: Pipeline) -> Vec<Option<Datum>> {
        // TODO: Right now we're collecting all the values upfront, which isn't the best call for
        // long running pipelines. Eventually we'll need to pass results up to the user as they
        // finish running through the pipeline;

        pipeline
            .data
            .iter()
            .map(|datum| {
                // Pass data through Filters
                let mut curr_datum: Option<Datum> = Some(datum.clone());
                for filter in pipeline.filters.iter() {
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
    fn exec(&self, datum: Datum) -> Result<Option<Datum>, std::io::Error>;
}

// A single piece of data from a Source that will be fed through the Pipeline.
// TODO: For simplicity data is just a number...we're going to have to create different types we
// can operate on.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Datum {
    data: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_run_empty_pipeline() {
        let data = vec![Datum { data: 4 }];
        let pipeline = Pipeline::new(data, vec![]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum { data: 4 }));
    }

    #[test]
    fn can_run_pipeline_with_filters() {
        struct Double;
        impl Filter for Double {
            fn exec(&self, datum: Datum) -> Result<Option<Datum>, std::io::Error> {
                Ok(Some(Datum {
                    data: datum.data * 2,
                }))
            }
        }

        let data = vec![Datum { data: 4 }];
        let pipeline = Pipeline::new(data, vec![Box::new(Double), Box::new(Double)]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(data_out[0], Some(Datum { data: 16 }));
    }
}
