//! builder for creating LineProtocol

use std::io::Write;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Must not contain duplicate tags, but "{}" was repeated"#, tag_key))]
    DuplicateTag { tag_key: String },
}


/// Writes line protocol to the output stream
pub struct LineProtocolBuilder<W> {
    writer: &W,
}

impl <W: Write> LineProtocolBuilder<W> {
    pub fn new(writer: & W) -> Self {
        Self { writer }
    }

    /// return the inner writer, consuming `self`
    pub fn into_inner(self) -> W {
        self.writer
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let writer = LineProtocolBuilder::new(&mut output);


}
