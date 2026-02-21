mod app;
mod domain;
mod io;
mod tui;

use anyhow::Result;
use app::App;

fn main() -> Result<()> {
    let mut app = App::new();
    app.run()
}
