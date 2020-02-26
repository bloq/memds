#[derive(Debug)]
pub enum MemdsError {
    InvalidFrame,
    ProtobufDecode,
    IO(std::io::Error),
}

impl From<std::io::Error> for MemdsError {
    fn from(err: std::io::Error) -> MemdsError {
        MemdsError::IO(err)
    }
}
