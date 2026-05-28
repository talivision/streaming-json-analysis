use anyhow::{bail, Context, Result};
use argh::FromArgs;
use json_analyzer::app::App;

#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use json_analyzer::control_http::spawn_control_http_server;
use json_analyzer::io::StreamReader;
use json_analyzer::persistence::{import_session, load_profile, Swapfile, SwapfileError};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// One of the supported input forms — a local path or an HTTP(S) URL.
#[derive(Clone)]
enum Input {
    Path(PathBuf),
    Url(String),
}

impl Input {
    fn from_path(p: PathBuf) -> Self {
        let s = p.to_string_lossy();
        if s.starts_with("http://") || s.starts_with("https://") {
            Self::Url(s.to_string())
        } else {
            Self::Path(p)
        }
    }

    fn source_id_for_swap(&self) -> String {
        match self {
            Input::Path(p) => json_analyzer::persistence::canonical_source_id(p),
            Input::Url(u) => u.clone(),
        }
    }

    fn display(&self) -> String {
        match self {
            Input::Path(p) => p.display().to_string(),
            Input::Url(u) => u.clone(),
        }
    }

    fn into_reader(self) -> StreamReader {
        match self {
            Input::Path(p) => StreamReader::from_path(p),
            Input::Url(u) => StreamReader::from_url(u),
        }
    }
}

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

    /// switch, escape C1 controls, DEL, and invisible Unicode in string values
    #[argh(switch)]
    escape_strings: bool,

    /// bind address for optional control HTTP API (e.g. 127.0.0.1:8080)
    #[argh(option)]
    control_http: Option<String>,

    /// switch, take over the swapfile even if another live process holds it
    /// (vim-style `-r` recovery — use only if you're certain the other
    /// process is dead)
    #[argh(switch)]
    force: bool,
}

fn main() -> Result<()> {
    let args: Args = argh::from_env();
    let control_http = args.control_http.clone();
    let force = args.force;
    if let Some(path) = args.baseline.as_ref() {
        ensure_input_path("--baseline", path)?;
    }
    if let Some(path) = args.import.as_ref() {
        ensure_input_path("--import", path)?;
    }
    if let Some(path) = args.profile.as_ref() {
        ensure_input_path("--profile", path)?;
    }
    if let Some(path) = args.whitelist.as_ref() {
        ensure_input_path("--whitelist", path)?;
    }
    if let Some(path) = args.path.as_ref() {
        ensure_input_path_or_url("<path>", path)?;
    }
    if let Some(path) = args.jsonl.as_ref() {
        ensure_input_path_or_url("--jsonl", path)?;
    }
    if let Some(import_path) = args.import.as_ref() {
        if args.path.is_some() || args.jsonl.is_some() {
            bail!("--import cannot be combined with <path> or --jsonl");
        }
        if args.baseline.is_some() {
            bail!("--import cannot be combined with --baseline");
        }
        let session = import_session(import_path)?;
        // Session imports replay a stored stream from the snapshot; the
        // source identity is whatever the export recorded (always a path
        // in practice). Don't try to interpret as URL.
        let stream_path = PathBuf::from(&session.stream_path);
        let swapfile = acquire_swapfile_or_exit(&Input::Path(stream_path.clone()), force)?;
        let mut app = App::new(
            stream_path,
            None,
            true,
            args.debug_status,
            true,
            args.escape_strings,
        );
        app.set_swapfile(swapfile);
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
        let mut _control_server = None;
        if let Some(bind_addr) = control_http.clone() {
            let (control_tx, control_rx) = mpsc::channel();
            _control_server = Some(spawn_control_http_server(bind_addr, control_tx)?);
            app.set_control_receiver(control_rx);
        }
        return app.run();
    }

    let input_raw = match (args.path, args.jsonl) {
        (Some(_), Some(_)) => {
            bail!("provide exactly one input source: <path> or --jsonl <path>")
        }
        (Some(path), None) | (None, Some(path)) => path,
        (None, None) => {
            bail!("an input is required: provide <path|url> or --jsonl <path|url>")
        }
    };
    let input = Input::from_path(input_raw);

    if let Input::Path(p) = &input {
        if p.is_dir() {
            bail!("directory input is no longer supported");
        }
    }
    let swapfile = acquire_swapfile_or_exit(&input, force)?;
    let mut app = App::from_reader(
        input.clone().into_reader(),
        args.baseline,
        args.offline,
        args.debug_status,
        args.reset,
        args.escape_strings,
    );
    app.set_swapfile(swapfile);
    if let Some(whitelist_path) = args.whitelist.as_ref() {
        let terms = read_whitelist_terms(whitelist_path)?;
        app.add_whitelist_terms(terms);
    }
    if let Some(profile_path) = args.profile.as_ref() {
        let profile = load_profile(profile_path)?;
        app.apply_profile(profile, true);
    }
    let mut _control_server = None;
    if let Some(bind_addr) = control_http {
        let (control_tx, control_rx) = mpsc::channel();
        _control_server = Some(spawn_control_http_server(bind_addr, control_tx)?);
        app.set_control_receiver(control_rx);
    }
    app.run()
}

fn acquire_swapfile_or_exit(input: &Input, force: bool) -> Result<Swapfile> {
    let id = input.source_id_for_swap();
    let _display = input.display();
    match Swapfile::acquire_for_id(&id, force) {
        Ok(swap) => Ok(swap),
        Err(SwapfileError::Held(conflict)) => {
            // The kernel holds the lock for us, so being "held" means
            // another instance is genuinely alive (a crashed process
            // would have released the lock).
            eprintln!(
                "\x1b[1;33mE325: ATTENTION\x1b[0m\n\
                 Another instance of json-analyzer is currently editing this stream:\n\
                   swapfile: {}\n\
                   pid:      {}\n\
                   host:     {}\n\
                   stream:   {}\n\
                 \n\
                 Attach to that terminal instead — two writers will race and corrupt your\n\
                 annotations. If you really do intend to run both, pass --force.",
                conflict.swap_path.display(),
                conflict.record.pid,
                conflict.record.hostname,
                conflict.record.stream_path,
            );
            std::process::exit(1);
        }
        Err(SwapfileError::Io(err)) => Err(err.context("failed to acquire swapfile")),
    }
}

fn ensure_input_path(flag: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        bail!("{} path does not exist: {}", flag, path.display());
    }
    Ok(())
}

/// Like `ensure_input_path` but tolerates http(s):// URLs unchanged.
fn ensure_input_path_or_url(flag: &str, path: &Path) -> Result<()> {
    let s = path.to_string_lossy();
    if s.starts_with("http://") || s.starts_with("https://") {
        return Ok(());
    }
    ensure_input_path(flag, path)
}

fn read_whitelist_terms(path: &PathBuf) -> Result<Vec<String>> {
    let body = fs::read_to_string(path)
        .with_context(|| format!("failed to read --whitelist {}", path.display()))?;
    Ok(body
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect())
}
