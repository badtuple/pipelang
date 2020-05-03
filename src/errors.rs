#[derive(Debug)]
pub enum Error {
    MalformedQuery(String),

    CannotPushToUnregisteredSource,
    CannotReadFromUnregisteredSource,
    FilterCannotProcessDataType,
}
