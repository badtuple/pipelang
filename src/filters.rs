use crate::{errors::Error, Datum, Filter};

#[derive(Clone)]
pub struct GreaterThan {
    pub op: i64,
}

impl Filter for GreaterThan {
    fn exec(&mut self, datum: Datum) -> Result<Option<Datum>, Error> {
        match datum {
            Datum::Integer(i) => {
                if i <= self.op {
                    return Ok(None);
                }

                Ok(Some(datum))
            }
            _ => Err(Error::FilterCannotProcessDataType),
        }
    }
}

#[derive(Clone)]
pub struct Batch {
    pub n: usize,
    pub in_progress: Vec<Datum>,
}

impl Filter for Batch {
    fn exec(&mut self, datum: Datum) -> Result<Option<Datum>, Error> {
        self.in_progress.push(datum);

        if self.in_progress.len() >= self.n {
            let out = Some(Datum::Vec(self.in_progress.clone()));
            self.in_progress = vec![];
            return Ok(out);
        }

        Ok(None)
    }
}
