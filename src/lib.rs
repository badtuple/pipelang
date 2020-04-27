pub struct Interpreter;

impl Interpreter {
    fn new() -> Self {
        Self {}
    }

    fn exec(&self, query: String) {
        // TODO: Parse query into AST

        // TODO: AST to runnable Pipeline
        let pipeline = Pipeline {
            data: vec![Datum { data: 4 }],
            filters: vec![],
        };

        self.exec_pipeline(pipeline);
    }

    fn exec_pipeline(&self, pipeline: Pipeline) -> Vec<Box<Option<Datum>>> {
        // TODO: Right now we're collecting all the values upfront, which isn't the best call for
        // long running pipelines. Eventually we'll need to pass results up to the user as they
        // finish running through the pipeline;

        let filters = pipeline.filters.clone();

        pipeline
            .data
            .into_iter()
            .map(|datum| {
                // Pass data through Filters
                let mut curr_datum: Box<Option<Datum>> = Box::new(Some(datum));
                for filter in filters.iter() {
                    curr_datum = (filter.func)(*curr_datum);
                }
                curr_datum.clone()
            })
            .filter(|d| d.is_some())
            .collect()
    }
}

struct Pipeline {
    data: Vec<Datum>,
    filters: Vec<Filter>,
}

impl Pipeline {
    fn new(data: Vec<Datum>, filters: Vec<Filter>) -> Self {
        Pipeline { data, filters }
    }
}

#[derive(Clone)]
struct Filter {
    name: String,
    func: fn(Option<Datum>) -> Box<Option<Datum>>,
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
        assert_eq!(*data_out[0], Some(Datum { data: 4 }));
    }

    fn can_run_pipeline_with_filters() {
        let double = Filter {
            name: "double".into(),
            func: |datum: Option<Datum>| Box::new(datum.map(|d| Datum { data: d.data * 2 })),
        };

        let data = vec![Datum { data: 4 }];
        let pipeline = Pipeline::new(data, vec![double.clone(), double]);

        let interpreter = Interpreter::new();
        let data_out = interpreter.exec_pipeline(pipeline);
        assert_eq!(data_out.len(), 1);
        assert_eq!(*data_out[0], Some(Datum { data: 16 }));
    }
}
