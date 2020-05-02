#[derive(Debug)]
pub enum Error {
    CannotPushToUnregisteredSource,
    CannotReadFromUnregisteredSource,
    FilterCannotProcessDataType,
}
