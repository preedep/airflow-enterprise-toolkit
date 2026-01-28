//! CLI binary entry point
//! 
//! This is the main entry point for the Airflow Enterprise Toolkit CLI application.

use airflow_enterprise_toolkit::presentation::cli::main as cli_main;

fn main() -> anyhow::Result<()> {
    cli_main()
}
