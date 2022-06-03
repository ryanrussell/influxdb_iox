use data_types::SequenceNumber;

/// Accumulates a minimim [`SequenceNumber`] from multiple [`Option<SequenceNumber>`]
#[derive(Debug, Clone)]
pub struct MinSequenceNumber(Option<SequenceNumber>);

impl MinSequenceNumber {
    pub fn new() -> Self {
        Self(None)
    }

    /// Update the inner value with `new_sequence_number`
    pub fn min(mut self, new_sequence_number: Option<SequenceNumber>) -> Self {
        let new_sequence_number = if let Some(val) = new_sequence_number {
            val
        } else {
            return self;
        };

        self.0 = Some(match self.0.take() {
            None => new_sequence_number,
            Some(val) => val.min(new_sequence_number),
        });

        self
    }

    /// Update the inner value repeatedly with items from `iter`
    pub fn min_iter(self, iter: impl IntoIterator<Item = Option<SequenceNumber>>) -> Self {
        iter.into_iter()
            .fold(self, |s, new_sequence_number| s.min(new_sequence_number))
    }

    /// Return the contained [`SequenceNumber`], if any
    pub fn into_inner(self) -> Option<SequenceNumber> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_none() {
        let sequence_number = MinSequenceNumber::new().min(None);
        assert_eq!(sequence_number.into_inner(), None);
    }

    #[test]
    fn min_val() {
        let sequence_number = MinSequenceNumber::new().min(Some(SequenceNumber::new(5)));
        assert_eq!(sequence_number.into_inner(), Some(SequenceNumber::new(5)));
    }

    #[test]
    fn min_val_then_none() {
        let sequence_number = MinSequenceNumber::new()
            .min(Some(SequenceNumber::new(5)))
            // expect no update after none
            .min(None);
        assert_eq!(sequence_number.into_inner(), Some(SequenceNumber::new(5)));
    }

    #[test]
    fn min_multi_val() {
        let sequence_number = MinSequenceNumber::new()
            .min(Some(SequenceNumber::new(4)))
            .min(Some(SequenceNumber::new(5)))
            .min(Some(SequenceNumber::new(3)));
        assert_eq!(sequence_number.into_inner(), Some(SequenceNumber::new(3)));
    }

    #[test]
    fn min_iter() {
        let sequence_number = MinSequenceNumber::new().min_iter(vec![
            Some(SequenceNumber::new(4)),
            Some(SequenceNumber::new(5)),
            Some(SequenceNumber::new(3)),
        ]);
        assert_eq!(sequence_number.into_inner(), Some(SequenceNumber::new(3)));
    }
}
