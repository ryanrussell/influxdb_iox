//! builder for creating LineProtocol

use std::{io::Write, fmt::Debug};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Can not write tag {}={} after fields"#, tag_key, tag_value))]
    TagAfterField { tag_key: String, tag_value: String },

    #[snafu(display(r#"Underlying IO error: {}"#, source))]
    IO { source: std::io::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;


impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::IO { source }
    }
}

/// Writes line protocol to the output stream
pub struct LineProtocolBuilder<W> {
    writer: W,
}

#[derive(Debug)]
enum LineState {
    /// Seen the measurement, now in tags
    ZeroTags,
    /// Seen at least one other tag
    OneOrMoreTag,
    /// Seen all tags, now in fields
    Fields,
}

impl <W: Write>  Debug for LineProtocolBuilder<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineProtocolBuilder")
            .field("writer", &"...")
            .finish()
    }
}

// Represents a partial line
pub struct LineBuilder<'a, W> {
    lp_builder: &'a mut LineProtocolBuilder<W>,
    line_state: LineState,
}

impl <'a, W>  Debug for LineBuilder<'a, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineBuilder")
            .field("lp_builder", &"...")
            .field("line_state", &self.line_state)
            .finish()
    }
}

impl <'a, W: Write> LineBuilder<'a, W> {
    pub fn try_new<'b>(lp_builder: &'a mut LineProtocolBuilder<W>, measurement: &'b str) -> Result<Self> {
        write!(lp_builder.inner_mut(), "{} ", measurement)?;
        Ok(Self {
            lp_builder,
            line_state: LineState::ZeroTags,
        })
    }

    pub fn tag(mut self, tag_key: &str, tag_value: &str) -> Result<Self> {
        // TODO proper check for empty value
        assert!(!tag_key.is_empty());
        assert!(!tag_value.is_empty());

        // TODO proper check for invalid values / escaping
        //assert!(

        match self.line_state {
            LineState::ZeroTags => {
                write!(self.writer(), "{}={}", tag_key, tag_value)?;
                self.line_state = LineState::OneOrMoreTag;
            },
            LineState::OneOrMoreTag => {
                write!(self.writer(), ",{}={}", tag_key, tag_value)?;
            },
            LineState::Fields => {
                return TagAfterFieldSnafu{tag_key, tag_value}.fail();
            }
        };

        Ok(self)
    }

    pub fn field<F>(mut self, field_key: &str, field_value: F) -> Result<Self> {


    /// return a reference to the inner writer
    fn writer(&mut self) -> &mut W {
        self.lp_builder.inner_mut()
    }

}

impl <W: Write> LineProtocolBuilder<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn line<'a>(&'a mut self, measurement: &str) -> Result<LineBuilder<'a, W>> {
        LineBuilder::try_new(self, measurement)
    }

    /// return the inner writer, consuming `self`
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// return a reference to the inner writer
    fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() -> Result<()>{
        let mut writer = LineProtocolBuilder::new(vec![]);

        writer
            .line("m1")?
            .tag("foo", "bar")?
            .tag("foo2", "blarg")?
            .field("f1", 43f64)?
            .field("f2", true)?
            .timestamp(123)?;


        let lp = String::from_utf8(writer.into_inner()).unwrap();

        assert_eq!(lp, "foo");
        Ok(())
    }

    // test writing two lines


    // teset empty tag name
    // teset empty tag value

    // teset empty field name

    // test trying to write escaped stuff

    // test not writing fields

    // test not writing timestamp

}
