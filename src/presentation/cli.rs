//! CLI interface for the Airflow Enterprise Toolkit
//! 
//! This module provides command-line interface for converting Control-M jobs
//! to Airflow DAGs, with professional error handling and user feedback.

use crate::application::services::{DagGenerator, JobValidator};
use crate::infrastructure::parsers::ControlMParser;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, warn};

/// Airflow Enterprise Toolkit - Control-M to Airflow Migration Tool
#[derive(Parser)]
#[command(name = "aet-cli")]
#[command(about = "Enterprise toolkit for converting Control-M jobs to Airflow DAGs")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "Airflow Enterprise Toolkit Team")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Output directory for generated files
    #[arg(short, long, global = true, default_value = "./output")]
    pub output: PathBuf,

    /// Configuration file path
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,
}

/// Available CLI commands
#[derive(Subcommand)]
pub enum Commands {
    /// Convert Control-M XML to Airflow DAG
    Convert {
        /// Input Control-M XML file
        #[arg(short, long)]
        input: PathBuf,

        /// Output DAG file name (without extension)
        #[arg(short, long)]
        output_name: Option<String>,

        /// Generate Python DAG file
        #[arg(long, default_value = "true")]
        python: bool,

        /// Generate JSON representation
        #[arg(long)]
        json: bool,

        /// Validate input before conversion
        #[arg(long, default_value = "true")]
        validate: bool,

        /// Include order date handling
        #[arg(long, default_value = "true")]
        include_order_date: bool,
    },

    /// Validate Control-M XML file
    Validate {
        /// Input Control-M XML file
        #[arg(short, long)]
        input: PathBuf,

        /// Strict validation mode
        #[arg(long)]
        strict: bool,
    },

    /// Generate example Control-M XML
    GenerateExample {
        /// Output file path
        #[arg(short, long)]
        output: PathBuf,

        /// Example type to generate
        #[arg(long, default_value = "basic")]
        example_type: String,
    },

    /// Show configuration
    Config {
        /// Show current configuration
        show_config: bool,
    },
}

/// CLI application runner
pub struct CliApp {
    /// CLI arguments
    args: Cli,
}

impl CliApp {
    /// Create a new CLI application
    pub fn new(args: Cli) -> Self {
        Self { args }
    }

    /// Run the CLI application
    pub fn run(&self) -> Result<()> {
        // Initialize logging
        self.init_logging();

        info!("Starting Airflow Enterprise Toolkit v{}", env!("CARGO_PKG_VERSION"));

        // Create output directory if it doesn't exist
        std::fs::create_dir_all(&self.args.output)
            .with_context(|| format!("Failed to create output directory: {:?}", self.args.output))?;

        // Execute command
        match &self.args.command {
            Commands::Convert {
                input,
                output_name,
                python,
                json,
                validate,
                include_order_date,
            } => self.handle_convert_command(input, output_name, *python, *json, *validate, *include_order_date)?,
            Commands::Validate { input, strict } => self.handle_validate_command(input, *strict)?,
            Commands::GenerateExample { output, example_type } => {
                self.handle_generate_example_command(output, example_type)?
            }
            Commands::Config { show_config: _ } => self.handle_config_command()?,
        }

        info!("Command completed successfully");
        Ok(())
    }

    /// Initialize logging based on verbosity level
    fn init_logging(&self) {
        let level = if self.args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        };

        tracing_subscriber::fmt()
            .with_max_level(level)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(false)
            .with_line_number(false)
            .init();
    }

    /// Handle the convert command
    fn handle_convert_command(
        &self,
        input: &PathBuf,
        output_name: &Option<String>,
        python: bool,
        json: bool,
        validate: bool,
        include_order_date: bool,
    ) -> Result<()> {
        info!("Converting Control-M XML from: {:?}", input);

        // Parse Control-M XML
        let parser = ControlMParser::new();
        let jobs = parser.parse_xml_file(input.to_str().unwrap())
            .with_context(|| "Failed to parse Control-M XML file")?;

        info!("Successfully parsed {} Control-M jobs", jobs.len());

        // Validate jobs if requested
        if validate {
            info!("Validating Control-M jobs...");
            JobValidator::validate_jobs(&jobs)
                .with_context(|| "Job validation failed")?;
            info!("All jobs validated successfully");
        }

        // Generate Airflow DAG
        let mut generator_config = crate::application::services::GenerationConfig::default();
        generator_config.include_order_date = include_order_date;
        
        let generator = DagGenerator::with_config(generator_config);
        let dag = generator.generate_dag(jobs)
            .with_context(|| "Failed to generate Airflow DAG")?;

        info!("Successfully generated Airflow DAG: {}", dag.dag_id);

        // Determine output file name
        let output_name = output_name
            .as_ref()
            .unwrap_or(&dag.dag_id)
            .to_string();

        // Generate Python DAG file
        if python {
            let python_generator = crate::infrastructure::generators::PythonDagGenerator::new();
            let python_code = python_generator.generate_python_dag(&dag)
                .with_context(|| "Failed to generate Python DAG")?;

            let python_file = self.args.output.join(format!("{}.py", output_name));
            std::fs::write(&python_file, python_code)
                .with_context(|| format!("Failed to write Python DAG file: {:?}", python_file))?;

            info!("Python DAG written to: {:?}", python_file);
        }

        // Generate JSON file if requested
        if json {
            let json_content = serde_json::to_string_pretty(&dag)
                .with_context(|| "Failed to serialize DAG to JSON")?;

            let json_file = self.args.output.join(format!("{}.json", output_name));
            std::fs::write(&json_file, json_content)
                .with_context(|| format!("Failed to write JSON file: {:?}", json_file))?;

            info!("JSON representation written to: {:?}", json_file);
        }

        Ok(())
    }

    /// Handle the validate command
    fn handle_validate_command(&self, input: &PathBuf, strict: bool) -> Result<()> {
        info!("Validating Control-M XML file: {:?}", input);

        let mut parser_config = crate::infrastructure::parsers::ParserConfig::default();
        parser_config.strict_validation = strict;
        
        let parser = ControlMParser::with_config(parser_config);
        let jobs = parser.parse_xml_file(input.to_str().unwrap())
            .with_context(|| "Failed to parse Control-M XML file")?;

        info!("Successfully parsed {} Control-M jobs", jobs.len());

        // Validate jobs
        JobValidator::validate_jobs(&jobs)
            .with_context(|| "Job validation failed")?;

        println!("✅ Validation successful! {} jobs are valid.", jobs.len());
        
        // Print job summary
        for job in &jobs {
            println!("  - {} ({})", job.name, self.format_job_type(&job.job_type));
        }

        Ok(())
    }

    /// Handle the generate example command
    fn handle_generate_example_command(&self, output: &PathBuf, example_type: &str) -> Result<()> {
        info!("Generating example Control-M XML: type={}", example_type);

        let example_xml = match example_type {
            "basic" => self.generate_basic_example(),
            "advanced" => self.generate_advanced_example(),
            "hold" => self.generate_hold_example(),
            _ => return Err(anyhow::anyhow!("Unknown example type: {}", example_type)),
        };

        std::fs::write(output, example_xml)
            .with_context(|| format!("Failed to write example file: {:?}", output))?;

        info!("Example Control-M XML written to: {:?}", output);
        Ok(())
    }

    /// Handle the config command
    fn handle_config_command(&self) -> Result<()> {
        println!("Airflow Enterprise Toolkit Configuration:");
        println!("  Version: {}", env!("CARGO_PKG_VERSION"));
        println!("  Output Directory: {:?}", self.args.output);
        println!("  Verbose Logging: {}", self.args.verbose);
        
        if let Some(config_file) = &self.args.config {
            println!("  Config File: {:?}", config_file);
        }

        Ok(())
    }

    /// Format job type for display
    fn format_job_type(&self, job_type: &crate::domain::entities::JobType) -> String {
        match job_type {
            crate::domain::entities::JobType::Command => "Command".to_string(),
            crate::domain::entities::JobType::Script { interpreter } => {
                format!("Script({})", interpreter)
            }
            crate::domain::entities::JobType::Database { db_type } => {
                format!("Database({})", db_type)
            }
            crate::domain::entities::JobType::FileTransfer => "FileTransfer".to_string(),
            crate::domain::entities::JobType::Custom(name) => format!("Custom({})", name),
        }
    }

    /// Generate basic example XML
    fn generate_basic_example(&self) -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CONTROLM>
    <FOLDER>
        <FOLDER_NAME>BasicExample</FOLDER_NAME>
        <DESCRIPTION>Basic Control-M example folder</DESCRIPTION>
        <JOB>
            <JOBNAME>DataExtraction</JOBNAME>
            <DESCRIPTION>Extract data from source system</DESCRIPTION>
            <COMMAND>python3 extract_data.py</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <TIMEOUT>30</TIMEOUT>
        </JOB>
        <JOB>
            <JOBNAME>DataTransformation</JOBNAME>
            <DESCRIPTION>Transform extracted data</DESCRIPTION>
            <COMMAND>python3 transform_data.py</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <TIMEOUT>45</TIMEOUT>
            <INCOND>
                <NAME>DataExtraction</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
        <JOB>
            <JOBNAME>DataLoading</JOBNAME>
            <DESCRIPTION>Load transformed data to warehouse</DESCRIPTION>
            <COMMAND>python3 load_data.py</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <TIMEOUT>60</TIMEOUT>
            <INCOND>
                <NAME>DataTransformation</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
    </FOLDER>
</CONTROLM>"#.to_string()
    }

    /// Generate advanced example XML
    fn generate_advanced_example(&self) -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CONTROLM>
    <FOLDER>
        <FOLDER_NAME>AdvancedExample</FOLDER_NAME>
        <DESCRIPTION>Advanced Control-M example with scheduling and variables</DESCRIPTION>
        <JOB>
            <JOBNAME>DailyReport</JOBNAME>
            <DESCRIPTION>Generate daily business report</DESCRIPTION>
            <COMMAND>python3 generate_report.py --date={{ORDER_DATE}}</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <SCHEDULE>
                <DAYS>DAILY</DAYS>
                <TIMES>08:00</TIMES>
                <ORDER_DATE>PreviousBusinessDay</ORDER_DATE>
            </SCHEDULE>
            <VARIABLE>
                <NAME>REPORT_TYPE</NAME>
                <VALUE>DAILY_SUMMARY</VALUE>
            </VARIABLE>
            <VARIABLE>
                <NAME>EMAIL_RECIPIENTS</NAME>
                <VALUE>team@company.com</VALUE>
            </VARIABLE>
        </JOB>
    </FOLDER>
</CONTROLM>"#.to_string()
    }

    /// Generate hold example XML
    fn generate_hold_example(&self) -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CONTROLM>
    <FOLDER>
        <FOLDER_NAME>HoldExample</FOLDER_NAME>
        <DESCRIPTION>Control-M example with hold conditions</DESCRIPTION>
        <JOB>
            <JOBNAME>WaitForFile</JOBNAME>
            <DESCRIPTION>Wait for input file arrival</DESCRIPTION>
            <COMMAND>echo "File received"</COMMAND>
            <JOB_TYPE>Command</JOB_TYPE>
            <INCOND>
                <NAME>FILE_HOLD:/data/input.txt</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
        <JOB>
            <JOBNAME>ProcessFile</JOBNAME>
            <DESCRIPTION>Process the received file</DESCRIPTION>
            <COMMAND>python3 process_file.py</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <INCOND>
                <NAME>WaitForFile</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
    </FOLDER>
</CONTROLM>"#.to_string()
    }
}

/// Main entry point for the CLI application
pub fn main() -> Result<()> {
    let cli = Cli::parse();
    let app = CliApp::new(cli);
    app.run()
}
