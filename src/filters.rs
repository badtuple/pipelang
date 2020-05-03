use crate::{errors::Error, Datum};

pub trait Filter: FilterClone {
    fn exec(&mut self, datum: Datum) -> Result<Option<Datum>, Error>;
}

// This is a hack to allow us to clone Filters that are passed around as trait objects.
// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object/30353928#30353928

pub trait FilterClone {
    fn clone_box(&self) -> Box<dyn Filter>;
}

impl<T> FilterClone for T
where
    T: 'static + Filter + Clone,
{
    fn clone_box(&self) -> Box<dyn Filter> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Filter> {
    fn clone(&self) -> Box<dyn Filter> {
        self.clone_box()
    }
}

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
