use anyhow::Result;
use json_analyzer::app::App;

fn main() -> Result<()> {
    let mut app = App::new();
    app.run()
}
