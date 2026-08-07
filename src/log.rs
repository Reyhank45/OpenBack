/// Structured daemon log macro.
/// Emits to **stderr** (never stdout / app output) in the format:
///   [Component](HH:MM:SS.mmm)<LEVEL> message
///
/// Usage:
///   dlog!("OverlayEngine", "INFO", "Mounting OverlayFS...");
///   dlog!("Daemon", "WARN", "Replica {} exited with code {}", name, code);
#[macro_export]
macro_rules! dlog {
    ($component:expr, $level:expr, $($arg:tt)*) => {{
        let _now = chrono::Local::now();
        eprintln!(
            "[{}]({}) <{}> {}",
            $component,
            _now.format("%H:%M:%S%.3f"),
            $level,
            format!($($arg)*)
        );
    }};
}
