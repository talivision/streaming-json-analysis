use anyhow::{bail, Result};
use argh::FromArgs;
use json_analyzer::app::App;
use std::path::PathBuf;

/// Analyze a JSONL stream in the terminal UI.
#[derive(FromArgs)]
struct Args {
    /// path to input JSONL file
    #[argh(positional)]
    path: Option<PathBuf>,

    /// path to input JSONL file
    #[argh(option)]
    jsonl: Option<PathBuf>,

    /// path to baseline JSONL file
    #[argh(option)]
    baseline: Option<PathBuf>,

    /// run without persisting baseline updates
    #[argh(switch)]
    offline: bool,
}

fn main() -> Result<()> {
    let args: Args = argh::from_env();
    let jsonl_path = match (args.path, args.jsonl) {
        (Some(_), Some(_)) => bail!("provide either <path> or --jsonl <path>, not both"),
        (Some(path), None) | (None, Some(path)) => path,
        (None, None) => bail!("a path is required: provide <path> or --jsonl <path>"),
    };

    let mut app = App::new(jsonl_path, args.baseline, args.offline);
    app.run()
}
