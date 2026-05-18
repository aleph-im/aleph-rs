use aleph_sdk::aggregate_models::vm_images::{
    ImageEntry, RootfsEntry, VmImageDefaults, VmImagesData,
};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Write;

pub fn render_diff(before: &VmImagesData, after: &VmImagesData) -> String {
    let mut out = String::new();
    render_rootfs_section(&mut out, &before.rootfs, &after.rootfs);
    render_image_section(&mut out, "runtimes", &before.runtimes, &after.runtimes);
    render_image_section(&mut out, "firmwares", &before.firmwares, &after.firmwares);
    render_defaults_section(&mut out, &before.defaults, &after.defaults);
    if out.is_empty() {
        out.push_str("(no changes)\n");
    }
    out
}

fn render_rootfs_section(
    out: &mut String,
    before: &BTreeMap<String, RootfsEntry>,
    after: &BTreeMap<String, RootfsEntry>,
) {
    let names: BTreeSet<&str> = before
        .keys()
        .chain(after.keys())
        .map(String::as_str)
        .collect();
    let mut body = String::new();
    for name in names {
        match (before.get(name), after.get(name)) {
            (None, Some(e)) => {
                let _ = writeln!(body, "  + {name}:");
                render_rootfs_full(&mut body, e, "      ");
            }
            (Some(e), None) => {
                let _ = writeln!(body, "  - {name}:");
                render_rootfs_full(&mut body, e, "      ");
            }
            (Some(b), Some(a)) => {
                let mut fields = String::new();
                render_rootfs_diff(&mut fields, b, a, "      ");
                if !fields.is_empty() {
                    let _ = writeln!(body, "  ~ {name}:");
                    body.push_str(&fields);
                }
            }
            (None, None) => unreachable!(),
        }
    }
    if !body.is_empty() {
        out.push_str("rootfs:\n");
        out.push_str(&body);
    }
}

fn render_image_section(
    out: &mut String,
    section: &str,
    before: &BTreeMap<String, ImageEntry>,
    after: &BTreeMap<String, ImageEntry>,
) {
    let names: BTreeSet<&str> = before
        .keys()
        .chain(after.keys())
        .map(String::as_str)
        .collect();
    let mut body = String::new();
    for name in names {
        match (before.get(name), after.get(name)) {
            (None, Some(e)) => {
                let _ = writeln!(body, "  + {name}:");
                render_image_full(&mut body, e, "      ");
            }
            (Some(e), None) => {
                let _ = writeln!(body, "  - {name}:");
                render_image_full(&mut body, e, "      ");
            }
            (Some(b), Some(a)) => {
                let mut fields = String::new();
                render_image_diff(&mut fields, b, a, "      ");
                if !fields.is_empty() {
                    let _ = writeln!(body, "  ~ {name}:");
                    body.push_str(&fields);
                }
            }
            (None, None) => unreachable!(),
        }
    }
    if !body.is_empty() {
        out.push_str(section);
        out.push_str(":\n");
        out.push_str(&body);
    }
}

fn render_defaults_section(out: &mut String, before: &VmImageDefaults, after: &VmImageDefaults) {
    let mut lines = String::new();
    render_default_line(&mut lines, "rootfs", &before.rootfs, &after.rootfs);
    render_default_line(&mut lines, "runtime", &before.runtime, &after.runtime);
    render_default_line(&mut lines, "firmware", &before.firmware, &after.firmware);
    if !lines.is_empty() {
        out.push_str("defaults:\n");
        out.push_str(&lines);
    }
}

fn render_default_line(
    out: &mut String,
    label: &str,
    before: &Option<String>,
    after: &Option<String>,
) {
    if before == after {
        return;
    }
    match (before, after) {
        (None, Some(v)) => {
            let _ = writeln!(out, "  + {label}: {v:?}");
        }
        (Some(v), None) => {
            let _ = writeln!(out, "  - {label}: was {v:?} (cleared)");
        }
        (Some(prev), Some(curr)) => {
            let _ = writeln!(out, "  ~ {label}: {prev:?} -> {curr:?}");
        }
        (None, None) => {}
    }
}

fn render_rootfs_full(out: &mut String, e: &RootfsEntry, indent: &str) {
    let _ = writeln!(out, "{indent}hash:         {}", e.hash);
    if let Some(v) = &e.display_name {
        let _ = writeln!(out, "{indent}display_name: {v}");
    }
    if let Some(v) = &e.description {
        let _ = writeln!(out, "{indent}description:  {v}");
    }
    if let Some(v) = e.min_disk_mib {
        let _ = writeln!(out, "{indent}min_disk_mib: {v}");
    }
    if e.deprecated {
        let _ = writeln!(out, "{indent}deprecated:   true");
    }
}

fn render_image_full(out: &mut String, e: &ImageEntry, indent: &str) {
    let _ = writeln!(out, "{indent}hash:         {}", e.hash);
    if let Some(v) = &e.display_name {
        let _ = writeln!(out, "{indent}display_name: {v}");
    }
    if let Some(v) = &e.description {
        let _ = writeln!(out, "{indent}description:  {v}");
    }
    if e.deprecated {
        let _ = writeln!(out, "{indent}deprecated:   true");
    }
}

fn render_rootfs_diff(out: &mut String, b: &RootfsEntry, a: &RootfsEntry, indent: &str) {
    if b.hash != a.hash {
        let _ = writeln!(out, "{indent}hash:         {} -> {}", b.hash, a.hash);
    }
    diff_opt_string(
        out,
        indent,
        "display_name",
        &b.display_name,
        &a.display_name,
    );
    diff_opt_string(out, indent, "description ", &b.description, &a.description);
    diff_opt_u64(out, indent, "min_disk_mib", b.min_disk_mib, a.min_disk_mib);
    if b.deprecated != a.deprecated {
        let _ = writeln!(
            out,
            "{indent}deprecated:   {} -> {}",
            b.deprecated, a.deprecated
        );
    }
}

fn render_image_diff(out: &mut String, b: &ImageEntry, a: &ImageEntry, indent: &str) {
    if b.hash != a.hash {
        let _ = writeln!(out, "{indent}hash:         {} -> {}", b.hash, a.hash);
    }
    diff_opt_string(
        out,
        indent,
        "display_name",
        &b.display_name,
        &a.display_name,
    );
    diff_opt_string(out, indent, "description ", &b.description, &a.description);
    if b.deprecated != a.deprecated {
        let _ = writeln!(
            out,
            "{indent}deprecated:   {} -> {}",
            b.deprecated, a.deprecated
        );
    }
}

fn diff_opt_string(
    out: &mut String,
    indent: &str,
    label: &str,
    before: &Option<String>,
    after: &Option<String>,
) {
    if before == after {
        return;
    }
    let _ = writeln!(
        out,
        "{indent}{label}: {} -> {}",
        opt_str(before),
        opt_str(after)
    );
}

fn diff_opt_u64(
    out: &mut String,
    indent: &str,
    label: &str,
    before: Option<u64>,
    after: Option<u64>,
) {
    if before == after {
        return;
    }
    let _ = writeln!(
        out,
        "{indent}{label}: {} -> {}",
        opt_num(before),
        opt_num(after)
    );
}

fn opt_str(v: &Option<String>) -> String {
    match v {
        Some(s) => format!("{s:?}"),
        None => "None".into(),
    }
}

fn opt_num(v: Option<u64>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "None".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::item_hash::ItemHash;
    use std::str::FromStr;

    fn h1() -> ItemHash {
        ItemHash::from_str("5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e")
            .unwrap()
    }
    fn h2() -> ItemHash {
        ItemHash::from_str("4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c")
            .unwrap()
    }

    #[test]
    fn diff_no_changes_renders_empty_marker() {
        let data = VmImagesData::default();
        let s = render_diff(&data, &data);
        assert_eq!(s, "(no changes)\n");
    }

    #[test]
    fn diff_add_rootfs_shows_plus_block() {
        let before = VmImagesData::default();
        let mut after = VmImagesData::default();
        after.rootfs.insert(
            "ubuntu24".into(),
            RootfsEntry {
                hash: h1(),
                display_name: Some("Ubuntu 24.04".into()),
                description: None,
                min_disk_mib: Some(20480),
                deprecated: false,
            },
        );
        let s = render_diff(&before, &after);
        assert!(s.contains("rootfs:"), "{s}");
        assert!(s.contains("  + ubuntu24:"), "{s}");
        assert!(s.contains("hash:         5330"), "{s}");
        assert!(s.contains("display_name: Ubuntu 24.04"), "{s}");
        assert!(s.contains("min_disk_mib: 20480"), "{s}");
    }

    #[test]
    fn diff_update_shows_per_field_old_to_new() {
        let mut before = VmImagesData::default();
        before.rootfs.insert(
            "ubuntu24".into(),
            RootfsEntry {
                hash: h1(),
                display_name: Some("Ubuntu 24".into()),
                description: None,
                min_disk_mib: Some(20480),
                deprecated: false,
            },
        );
        let mut after = before.clone();
        after.rootfs.get_mut("ubuntu24").unwrap().display_name = Some("Ubuntu 24.04 LTS".into());
        after.rootfs.get_mut("ubuntu24").unwrap().min_disk_mib = Some(24576);

        let s = render_diff(&before, &after);
        assert!(s.contains("~ ubuntu24:"), "{s}");
        assert!(
            s.contains(r#"display_name: "Ubuntu 24" -> "Ubuntu 24.04 LTS""#),
            "{s}"
        );
        assert!(s.contains("min_disk_mib: 20480 -> 24576"), "{s}");
    }

    #[test]
    fn diff_clear_field_shows_explicit_none() {
        let mut before = VmImagesData::default();
        before.rootfs.insert(
            "ubuntu24".into(),
            RootfsEntry {
                hash: h1(),
                display_name: Some("Ubuntu".into()),
                description: None,
                min_disk_mib: None,
                deprecated: false,
            },
        );
        let mut after = before.clone();
        after.rootfs.get_mut("ubuntu24").unwrap().display_name = None;

        let s = render_diff(&before, &after);
        assert!(s.contains(r#"display_name: "Ubuntu" -> None"#), "{s}");
    }

    #[test]
    fn diff_deprecate_shows_bool_flip() {
        let mut before = VmImagesData::default();
        before.rootfs.insert(
            "ubuntu24".into(),
            RootfsEntry {
                hash: h1(),
                display_name: None,
                description: None,
                min_disk_mib: None,
                deprecated: false,
            },
        );
        let mut after = before.clone();
        after.rootfs.get_mut("ubuntu24").unwrap().deprecated = true;

        let s = render_diff(&before, &after);
        assert!(s.contains("deprecated:   false -> true"), "{s}");
    }

    #[test]
    fn diff_set_default_shows_pointer_change() {
        let mut before = VmImagesData::default();
        before.defaults.rootfs = Some("ubuntu22".into());
        let mut after = before.clone();
        after.defaults.rootfs = Some("ubuntu24".into());

        let s = render_diff(&before, &after);
        assert!(s.contains("defaults:"), "{s}");
        assert!(s.contains(r#"~ rootfs: "ubuntu22" -> "ubuntu24""#), "{s}");
    }

    #[test]
    fn diff_clear_default_shows_was_value() {
        let mut before = VmImagesData::default();
        before.defaults.runtime = Some("py311".into());
        let after = VmImagesData::default();

        let s = render_diff(&before, &after);
        assert!(s.contains(r#"- runtime: was "py311" (cleared)"#), "{s}");
    }

    #[test]
    fn diff_runtime_change_renders_under_runtimes_section() {
        let mut before = VmImagesData::default();
        before.runtimes.insert(
            "py311".into(),
            ImageEntry {
                hash: h1(),
                display_name: Some("3.11".into()),
                description: None,
                deprecated: false,
            },
        );
        let mut after = before.clone();
        after.runtimes.get_mut("py311").unwrap().hash = h2();
        let s = render_diff(&before, &after);
        assert!(s.contains("runtimes:"), "{s}");
        assert!(s.contains("~ py311:"), "{s}");
        assert!(s.contains("hash:"), "{s}");
        assert!(s.contains("5330"), "{s}");
        assert!(s.contains("4a0f"), "{s}");
    }
}
