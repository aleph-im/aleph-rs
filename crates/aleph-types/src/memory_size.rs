/// Convert Gigabytes to Mebibytes (the unit used for VM volumes).
/// Rounds up to ensure that data of a given size will fit in the space allocated.
pub const fn gigabyte_to_mebibyte(gb: u64) -> u64 {
    let mebibyte = (1 << 20) as f64;
    let gigabyte = 1_000_000_000f64;

    let result = gb as f64 * gigabyte / mebibyte;
    result.ceil() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gigabyte_to_mebibyte() {
        let mib = gigabyte_to_mebibyte(20);
        assert_eq!(mib, 19074);
    }
}
