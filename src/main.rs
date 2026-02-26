use anyhow::{bail, Result};
use argh::FromArgs;
use json_analyzer::app::App;
use json_analyzer::persistence::{import_session, load_profile};
use std::fs;
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

    /// path to input directory of JSON files
    #[argh(option)]
    directory: Option<PathBuf>,

    /// path to baseline JSONL file
    #[argh(option)]
    baseline: Option<PathBuf>,

    /// import a previously exported analysis session (offline mode)
    #[argh(option)]
    import: Option<PathBuf>,

    /// path to source profile JSON
    #[argh(option)]
    profile: Option<PathBuf>,

    /// newline-separated whitelist terms
    #[argh(option)]
    whitelist: Option<PathBuf>,

    /// switch, run without persisting baseline updates
    #[argh(switch)]
    offline: bool,

    /// switch, show internal status line details continuously
    #[argh(switch)]
    debug_status: bool,

    /// switch, start without loading persisted state from disk
    #[argh(switch)]
    reset: bool,
}

fn main() -> Result<()> {
    let args: Args = argh::from_env();
    if let Some(import_path) = args.import.as_ref() {
        if args.path.is_some() || args.jsonl.is_some() || args.directory.is_some() {
            bail!("--import cannot be combined with <path>, --jsonl, or --directory");
        }
        if args.baseline.is_some() {
            bail!("--import cannot be combined with --baseline");
        }
        let session = import_session(import_path)?;
        let stream_path = PathBuf::from(&session.stream_path);
        // Session import is self-contained; do not load persisted local state.
        let mut app = App::new(stream_path, None, true, args.debug_status, true);
        if let Some(whitelist_path) = args.whitelist.as_ref() {
            let terms = read_whitelist_terms(whitelist_path)?;
            app.add_whitelist_terms(terms);
        }
        let profile_override = if let Some(profile_path) = args.profile.as_ref() {
            Some(load_profile(profile_path)?)
        } else {
            None
        };
        app.import_session(session, profile_override)?;
        return app.run();
    }

    let input_path = match (args.path, args.jsonl, args.directory) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => bail!(
            "provide exactly one input source: <path>, --jsonl <path>, or --directory <path>"
        ),
        (Some(path), None, None) | (None, Some(path), None) | (None, None, Some(path)) => path,
        (None, None, None) => bail!(
            "an input is required: provide <path>, --jsonl <path>, or --directory <path>"
        ),
    };

    if input_path.is_dir() && !args.offline {
        bail!("directory input requires --offline (live directory streaming is not supported)");
    }
    let mut app = App::new(
        input_path,
        args.baseline,
        args.offline,
        args.debug_status,
        args.reset,
    );
    if let Some(whitelist_path) = args.whitelist.as_ref() {
        let terms = read_whitelist_terms(whitelist_path)?;
        app.add_whitelist_terms(terms);
    }
    if let Some(profile_path) = args.profile.as_ref() {
        let profile = load_profile(profile_path)?;
        app.apply_profile(profile, true);
    }
    app.run()
}

fn read_whitelist_terms(path: &PathBuf) -> Result<Vec<String>> {
    let body = fs::read_to_string(path)?;
    Ok(body
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect())
}
