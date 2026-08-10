use crate::rpc::BaseManifest;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub fn inspect_base_image(base_path: &Path) -> BaseManifest {
    let os = detect_os(base_path);
    let libc = detect_libc(base_path);
    let architecture = detect_arch(base_path);

    BaseManifest {
        os,
        libc,
        architecture,
    }
}

fn detect_os(base_path: &Path) -> String {
    let os_release_path = base_path.join("etc").join("os-release");
    if let Ok(content) = std::fs::read_to_string(os_release_path) {
        for line in content.lines() {
            if line.starts_with("ID=") {
                return line.trim_start_matches("ID=").trim_matches('"').to_string();
            }
        }
    }
    "unknown".to_string()
}

fn detect_libc(base_path: &Path) -> String {
    // Check for musl
    if let Ok(entries) = std::fs::read_dir(base_path.join("lib")) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("ld-musl-") {
                    return "musl".to_string();
                }
            }
        }
    }
    if let Ok(entries) = std::fs::read_dir(base_path.join("lib64")) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("ld-musl-") {
                    return "musl".to_string();
                }
            }
        }
    }

    // Check for glibc
    if base_path.join("lib").join("x86_64-linux-gnu").join("libc.so.6").exists() 
        || base_path.join("lib64").join("ld-linux-x86-64.so.2").exists() 
        || base_path.join("lib").join("aarch64-linux-gnu").join("libc.so.6").exists()
        || base_path.join("lib").join("libc.so.6").exists() {
        return "glibc".to_string();
    }

    "unknown".to_string()
}

fn detect_arch(base_path: &Path) -> String {
    let sh_paths = [
        base_path.join("bin").join("sh"),
        base_path.join("usr").join("bin").join("sh"),
    ];

    for path in sh_paths {
        if let Ok(mut file) = File::open(path) {
            let mut header = [0u8; 20];
            if file.read_exact(&mut header).is_ok() {
                if header[0..4] == [0x7f, 0x45, 0x4c, 0x46] {
                    // e_machine is at offset 0x12 (18)
                    let e_machine = u16::from_le_bytes([header[18], header[19]]);
                    match e_machine {
                        0x3E => return "x86_64".to_string(),
                        0xB7 => return "aarch64".to_string(),
                        _ => return format!("unknown-elf-0x{:X}", e_machine),
                    }
                }
            }
        }
    }
    "unknown".to_string()
}
