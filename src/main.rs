use anyhow::{bail, Result};
use json_analyzer::app::App;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).peekable();
    let mut jsonl_path: Option<PathBuf> = None;
    let mut baseline_path: Option<PathBuf> = None;
    let mut offline = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--jsonl" => {
                let path = args.next().unwrap_or_else(|| {
                    eprintln!("error: --jsonl requires a path argument");
                    std::process::exit(1);
                });
                jsonl_path = Some(PathBuf::from(path));
            }
            "--baseline" => {
                let path = args.next().unwrap_or_else(|| {
                    eprintln!("error: --baseline requires a path argument");
                    std::process::exit(1);
                });
                baseline_path = Some(PathBuf::from(path));
            }
            "--offline" => offline = true,
            other if !other.starts_with('-') => {
                jsonl_path = Some(PathBuf::from(other));
            }
            other => {
                eprintln!("error: unknown argument '{other}'");
                eprintln!("usage: json_analyzer <path> [--baseline <path>] [--offline]");
                std::process::exit(1);
            }
        }
    }

    let Some(path) = jsonl_path else {
        bail!("a path is required\nusage: json_analyzer <path> [--baseline <path>] [--offline]");
    };

    let mut app = App::new(path, baseline_path, offline);
    app.run()
}
