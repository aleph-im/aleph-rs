//! Generic open/closed ranges with multi-range arithmetic.
//!
//! Port of `src/aleph/toolkit/range.py`.
//!
//! Python uses a custom [Range] type with overlap, intersection, and
//! subtraction. We mirror that behaviour for any totally-ordered `T: Ord +
//! Clone + Display + Debug`. `Range<T>` and `MultiRange<T>` are equivalent to
//! Python's `Range` and `MultiRange` classes.

use std::fmt::{self, Debug, Display};

/// Half-open or closed range `[lower, upper]`, `[lower, upper)`, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Range<T: Ord + Clone> {
    pub lower: T,
    pub upper: T,
    pub lower_inc: bool,
    pub upper_inc: bool,
}

impl<T: Ord + Clone + Display> Display for Range<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let left = if self.lower_inc { '[' } else { '(' };
        let right = if self.upper_inc { ']' } else { ')' };
        write!(f, "{left}{},{}{right}", self.lower, self.upper)
    }
}

impl<T: Ord + Clone> Range<T> {
    /// Construct a new [`Range`]. Mirrors `Range.__init__`.
    ///
    /// Returns `Err` if `upper < lower`, matching Python's `ValueError`.
    pub fn new(lower: T, upper: T, lower_inc: bool, upper_inc: bool) -> Result<Self, &'static str> {
        if upper < lower {
            return Err("Range start must be lower than range end");
        }
        Ok(Self {
            lower,
            upper,
            lower_inc,
            upper_inc,
        })
    }

    /// Inclusive-lower / exclusive-upper convenience constructor.
    pub fn closed_open(lower: T, upper: T) -> Result<Self, &'static str> {
        Self::new(lower, upper, true, false)
    }

    /// Parse `[1,10]`, `(1,10)`, `[1,10)`, etc. via a `parser` callback,
    /// mirroring `Range.from_str`.
    pub fn from_str<F, E>(range_str: &str, parser: F) -> Result<Self, String>
    where
        F: Fn(&str) -> Result<T, E>,
        E: Display,
    {
        if range_str.len() < 4 {
            return Err(format!("Invalid range string: {range_str}"));
        }
        let bytes = range_str.as_bytes();
        let left = bytes[0] as char;
        let right = bytes[range_str.len() - 1] as char;
        let inner = &range_str[1..range_str.len() - 1];
        let (lo, hi) = inner
            .split_once(',')
            .ok_or_else(|| format!("Invalid range string: {range_str}"))?;
        let lower = parser(lo).map_err(|e| e.to_string())?;
        let upper = parser(hi).map_err(|e| e.to_string())?;
        let lower_inc = left == '[';
        let upper_inc = right == ']';
        Self::new(lower, upper, lower_inc, upper_inc).map_err(str::to_string)
    }

    /// `self` is strictly to the left of `other` (no overlap).
    pub fn is_strictly_left_of(&self, other: &Range<T>) -> bool {
        if self.upper_inc || other.lower_inc {
            self.upper < other.lower
        } else {
            self.upper <= other.lower
        }
    }

    /// `self` is strictly to the right of `other`.
    pub fn is_strictly_right_of(&self, other: &Range<T>) -> bool {
        other.is_strictly_left_of(self)
    }

    /// Determine whether the two ranges share any point.
    pub fn overlaps(&self, other: &Range<T>) -> bool {
        if self.lower > other.upper || self.upper < other.lower {
            return false;
        }
        if self.lower < other.lower {
            if self.upper > other.lower {
                return true;
            } else if self.upper == other.lower && (self.upper_inc || other.lower_inc) {
                return true;
            } else {
                return false;
            }
        } else if self.lower == other.lower && (self.lower_inc || other.lower_inc) {
            return true;
        } else if self.lower < other.upper {
            return true;
        } else if self.lower == other.upper && (self.lower_inc || other.upper_inc) {
            return true;
        }
        false
    }

    /// Combine two overlapping ranges into one (Python's `__add__`).
    /// If they don't overlap, returns a clone of `self`.
    pub fn merge(&self, other: &Range<T>) -> Range<T> {
        if self.overlaps(other) {
            let lower = self.lower.clone().min(other.lower.clone());
            let upper = self.upper.clone().max(other.upper.clone());
            // Python `Range(min, max)` uses defaults `[lower_inc=True, upper_inc=False]`.
            Range::new(lower, upper, true, false).expect("merge bounds valid")
        } else {
            self.clone()
        }
    }

    /// Compute `self - other` as a (possibly empty) list of ranges.
    /// Mirrors Python's `Range.__sub__`.
    pub fn subtract(&self, other: &Range<T>) -> Vec<Range<T>> {
        if !self.overlaps(other) {
            return vec![self.clone()];
        }

        if self.lower < other.lower {
            if self.upper <= other.upper {
                vec![
                    Range::new(
                        self.lower.clone(),
                        other.lower.clone(),
                        self.lower_inc,
                        !other.lower_inc,
                    )
                    .expect("subtract bounds valid"),
                ]
            } else {
                vec![
                    Range::new(
                        self.lower.clone(),
                        other.lower.clone(),
                        self.lower_inc,
                        !other.lower_inc,
                    )
                    .expect("subtract bounds valid"),
                    Range::new(
                        other.upper.clone(),
                        self.upper.clone(),
                        !other.upper_inc,
                        self.upper_inc,
                    )
                    .expect("subtract bounds valid"),
                ]
            }
        } else if self.upper > other.upper {
            vec![
                Range::new(
                    other.upper.clone(),
                    self.upper.clone(),
                    !other.upper_inc,
                    self.upper_inc,
                )
                .expect("subtract bounds valid"),
            ]
        } else if self.upper == other.upper && (self.upper_inc && !other.upper_inc) {
            // Edge case ex: [1, 10] - [1, 10) = [10, 10]
            vec![
                Range::new(
                    other.upper.clone(),
                    self.upper.clone(),
                    !other.upper_inc,
                    self.upper_inc,
                )
                .expect("subtract bounds valid"),
            ]
        } else {
            vec![]
        }
    }

    /// `self - multirange`. Mirrors Python's `Range.remove_multirange`.
    pub fn remove_multirange(&self, multirange: &MultiRange<T>) -> Vec<Range<T>> {
        let mut missing_ranges = vec![self.clone()];
        for rng in &multirange.ranges {
            let mut next = Vec::new();
            for missing in &missing_ranges {
                next.extend(missing.subtract(rng));
            }
            missing_ranges = next;
        }
        missing_ranges
    }
}

/// Specialisation matching Python's `int_range(range_str)` helper.
pub fn int_range(range_str: &str) -> Result<Range<i64>, String> {
    Range::from_str(range_str, |s| s.trim().parse::<i64>())
}

/// Ordered, disjoint-ish collection of ranges. Mirrors `MultiRange` in Python.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MultiRange<T: Ord + Clone> {
    pub ranges: Vec<Range<T>>,
}

impl<T: Ord + Clone + Display> Display for MultiRange<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Multirange(")?;
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{r}")?;
        }
        write!(f, ")")
    }
}

impl<T: Ord + Clone> MultiRange<T> {
    /// Build a multirange. Ranges are sorted by `lower` (matching Python).
    pub fn new(mut ranges: Vec<Range<T>>) -> Self {
        ranges.sort_by(|a, b| a.lower.cmp(&b.lower));
        Self { ranges }
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Range<T>> {
        self.ranges.iter()
    }

    /// Insert `other`, merging into existing overlapping ranges, mirroring
    /// `MultiRange.add_range`.
    pub fn add_range(&mut self, other: Range<T>) {
        let mut accumulator = other;
        let mut left_ranges = Vec::new();
        let mut right_ranges = Vec::new();
        let owned = std::mem::take(&mut self.ranges);
        for rng in owned {
            if accumulator.overlaps(&rng) {
                accumulator = accumulator.merge(&rng);
            } else if rng.is_strictly_left_of(&accumulator) {
                left_ranges.push(rng);
            } else {
                right_ranges.push(rng);
            }
        }
        left_ranges.push(accumulator);
        left_ranges.extend(right_ranges);
        self.ranges = left_ranges;
    }

    /// `self - other` mirrors Python's `MultiRange.__sub__`.
    pub fn subtract(&self, other: &MultiRange<T>) -> MultiRange<T> {
        let mut missing_ranges = Vec::new();
        for rng in &self.ranges {
            missing_ranges.extend(rng.remove_multirange(other));
        }
        MultiRange::new(missing_ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_range_parsing() {
        let r = int_range("[1,10)").unwrap();
        assert_eq!(r.lower, 1);
        assert_eq!(r.upper, 10);
        assert!(r.lower_inc);
        assert!(!r.upper_inc);
    }

    #[test]
    fn test_overlaps_and_merge() {
        let a = Range::new(1, 10, true, true).unwrap();
        let b = Range::new(5, 15, true, true).unwrap();
        let c = Range::new(20, 30, true, true).unwrap();
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
        let merged = a.merge(&b);
        assert_eq!(merged.lower, 1);
        assert_eq!(merged.upper, 15);
    }

    #[test]
    fn test_subtract() {
        let a = Range::new(1, 10, true, true).unwrap();
        let b = Range::new(5, 7, true, true).unwrap();
        let parts = a.subtract(&b);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].lower, 1);
        assert_eq!(parts[0].upper, 5);
        assert_eq!(parts[1].lower, 7);
        assert_eq!(parts[1].upper, 10);
    }

    #[test]
    fn test_subtract_inclusive_edge() {
        // [1, 10] - [1, 10) = [10, 10]
        let a = Range::new(1, 10, true, true).unwrap();
        let b = Range::new(1, 10, true, false).unwrap();
        let parts = a.subtract(&b);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].lower, 10);
        assert_eq!(parts[0].upper, 10);
        assert!(parts[0].lower_inc);
        assert!(parts[0].upper_inc);
    }

    #[test]
    fn test_strictly_left_right() {
        // (..,5) and (5,..) are strictly disjoint when neither endpoint is
        // inclusive at 5. Mirrors Python's `is_strictly_left_of`.
        let a = Range::new(1, 5, true, false).unwrap();
        let b = Range::new(5, 10, false, false).unwrap();
        assert!(a.is_strictly_left_of(&b));
        assert!(b.is_strictly_right_of(&a));

        // If `other.lower_inc=true`, ranges touching at 5 are NOT strictly
        // left of each other (they share that point).
        let b_inc = Range::new(5, 10, true, false).unwrap();
        assert!(!a.is_strictly_left_of(&b_inc));
    }

    #[test]
    fn test_multirange_add_and_subtract() {
        let mut m = MultiRange::new(vec![
            Range::new(1, 5, true, false).unwrap(),
            Range::new(10, 15, true, false).unwrap(),
        ]);
        m.add_range(Range::new(20, 25, true, false).unwrap());
        assert_eq!(m.len(), 3);

        let n = MultiRange::new(vec![Range::new(3, 12, true, false).unwrap()]);
        let diff = m.subtract(&n);
        // [1,3) , [12,15), [20,25)
        assert_eq!(diff.len(), 3);
    }

    #[test]
    fn test_invalid_range_errors() {
        assert!(Range::new(10, 1, true, true).is_err());
    }
}
