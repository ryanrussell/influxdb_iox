//! This module contains code to pack values into a format suitable
//! for feeding to the parquet writer. It is destined for replacement
//! by an Apache Arrow based implementation

// Note the maintainability of this code is not likely high (it came
// from the copy pasta factory) but the plan is to replace it
// soon... We'll see how long that actually takes...
use parquet::data_type::ByteArray;

// NOTE: See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
// for an explanation of nesting levels

pub enum Packers {
    Float(Packer<f64>),
    Integer(Packer<i64>),
    String(Packer<ByteArray>),
    Boolean(Packer<bool>),
}

macro_rules! typed_packer_accessors {
    ($(($name:ident, $name_mut:ident, $type:ty, $variant:ident),)*) => {
        $(
            pub fn $name(&self) -> &Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant)));
                }
            }

            pub fn $name_mut(&mut self) -> &mut Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant)));
                }
            }
        )*
    };
}

impl Packers {
    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted into the Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Float(p) => p.reserve_exact(additional),
            Self::Integer(p) => p.reserve_exact(additional),
            Self::String(p) => p.reserve_exact(additional),
            Self::Boolean(p) => p.reserve_exact(additional),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Float(p) => p.is_empty(),
            Self::Integer(p) => p.is_empty(),
            Self::String(p) => p.is_empty(),
            Self::Boolean(p) => p.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Float(p) => p.len(),
            Self::Integer(p) => p.len(),
            Self::String(p) => p.len(),
            Self::Boolean(p) => p.len(),
        }
    }

    pub fn push_none(&mut self) {
        match self {
            Self::Float(p) => p.push_option(None),
            Self::Integer(p) => p.push_option(None),
            Self::String(p) => p.push_option(None),
            Self::Boolean(p) => p.push_option(None),
        }
    }

    /// Determines if the value for `row` is null is null.
    ///
    /// If there is no row then `is_null` returns `true`.
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Float(p) => p.is_null(row),
            Self::Integer(p) => p.is_null(row),
            Self::String(p) => p.is_null(row),
            Self::Boolean(p) => p.is_null(row),
        }
    }

    // Implementations of all the accessors for the variants of `Packers`.
    typed_packer_accessors! {
        (f64_packer, f64_packer_mut, f64, Float),
        (i64_packer, i64_packer_mut, i64, Integer),
        (str_packer, str_packer_mut, ByteArray, String),
        (bool_packer, bool_packer_mut, bool, Boolean),
    }
}

impl std::convert::From<delorean_table_schema::DataType> for Packers {
    fn from(t: delorean_table_schema::DataType) -> Self {
        match t {
            delorean_table_schema::DataType::Float => Packers::Float(Packer::<f64>::new()),
            delorean_table_schema::DataType::Integer => Packers::Integer(Packer::<i64>::new()),
            delorean_table_schema::DataType::String => Packers::String(Packer::<ByteArray>::new()),
            delorean_table_schema::DataType::Boolean => Packers::Boolean(Packer::<bool>::new()),
            delorean_table_schema::DataType::Timestamp => Packers::Integer(Packer::<i64>::new()),
        }
    }
}

#[derive(Debug, Default)]
pub struct Packer<T: PackerDefault> {
    values: Vec<T>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

impl<T: PackerDefault> Packer<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    /// Create a new packer with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted to the `Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.values.reserve_exact(additional);
        self.def_levels.reserve_exact(additional);
        self.rep_levels.reserve_exact(additional);
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.values.get(index)
    }

    // TODO(edd): I don't like these getters. They're only needed so we can
    // write the data into a parquet writer. We should have a method on Packer
    // that accepts some implementation of a trait that a parquet writer satisfies
    // and then pass the data through in here.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn def_levels(&self) -> &[i16] {
        &self.def_levels
    }

    pub fn rep_levels(&self) -> &[i16] {
        &self.rep_levels
    }

    pub fn push_option(&mut self, value: Option<T>) {
        match value {
            Some(v) => self.push(v),
            None => {
                self.values.push(T::default()); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }

    pub fn push(&mut self, value: T) {
        self.values.push(value);
        self.def_levels.push(1);
        self.rep_levels.push(1);
    }

    /// Return true if the row for index is null. Returns true if there is no
    /// row for index.
    pub fn is_null(&self, index: usize) -> bool {
        self.def_levels.get(index).map_or(true, |&x| x == 0)
    }
}

/// Provides a `Default` implementation of compatible `Packer` types where the
/// default values are compatible with the Parquet storage format.
///
/// TODO(edd): if we refactor out `ByteArray` as the string-based type the we
/// could probably get rid of this trait and `Packer` could derive `Default`.
pub trait PackerDefault {
    fn default() -> Self;
}

impl PackerDefault for f64 {
    fn default() -> Self {
        0.0
    }
}

impl PackerDefault for i64 {
    fn default() -> Self {
        0
    }
}

impl PackerDefault for ByteArray {
    fn default() -> Self {
        ByteArray::from("")
    }
}

impl PackerDefault for bool {
    fn default() -> Self {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn with_capacity() {
        let packer: Packer<bool> = Packer::with_capacity(42);
        assert_eq!(packer.values.capacity(), 42);
        assert_eq!(packer.def_levels.capacity(), 42);
        assert_eq!(packer.rep_levels.capacity(), 42);
    }

    #[test]
    fn is_null() {
        let mut packer: Packer<f64> = Packer::new();
        packer.push(22.3);
        packer.push_option(Some(100.3));
        packer.push_option(None);
        packer.push(33.3);

        assert_eq!(packer.is_null(0), false);
        assert_eq!(packer.is_null(1), false);
        assert_eq!(packer.is_null(2), true);
        assert_eq!(packer.is_null(3), false);
        assert_eq!(packer.is_null(4), true); // out of bounds
    }

    #[test]
    fn packers() {
        let mut packers: Vec<Packers> = Vec::new();
        packers.push(Packers::Float(Packer::new()));
        packers.push(Packers::Integer(Packer::new()));
        packers.push(Packers::Boolean(Packer::new()));

        packers.get_mut(0).unwrap().f64_packer_mut().push(22.033);
    }
}
