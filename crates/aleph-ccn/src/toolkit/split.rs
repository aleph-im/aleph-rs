//! Partition an iterable by predicate.
//!
//! Port of `src/aleph/toolkit/split.py`.

/// Split an iterable into two vectors: items matching `cond` and the rest.
///
/// Mirrors `split_iterable(iterable, cond) -> (matches, others)`.
pub fn split_iterable<I, T, F>(iterable: I, mut cond: F) -> (Vec<T>, Vec<T>)
where
    I: IntoIterator<Item = T>,
    F: FnMut(&T) -> bool,
{
    let mut matches = Vec::new();
    let mut others = Vec::new();
    for x in iterable {
        if cond(&x) {
            matches.push(x);
        } else {
            others.push(x);
        }
    }
    (matches, others)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_even_odd() {
        let (even, odd) = split_iterable(0..6, |x| x % 2 == 0);
        assert_eq!(even, vec![0, 2, 4]);
        assert_eq!(odd, vec![1, 3, 5]);
    }

    #[test]
    fn test_split_all_match() {
        let (m, o) = split_iterable(vec![1, 1, 1], |_| true);
        assert_eq!(m, vec![1, 1, 1]);
        assert!(o.is_empty());
    }

    #[test]
    fn test_split_none_match() {
        let (m, o) = split_iterable(vec![1, 2, 3], |_| false);
        assert!(m.is_empty());
        assert_eq!(o, vec![1, 2, 3]);
    }

    #[test]
    fn test_split_empty() {
        let (m, o): (Vec<i32>, Vec<i32>) = split_iterable(std::iter::empty(), |_| true);
        assert!(m.is_empty());
        assert!(o.is_empty());
    }
}
