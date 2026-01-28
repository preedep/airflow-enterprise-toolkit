//! # Airflow Enterprise Toolkit
//! 
//! A comprehensive toolkit for converting Control-M jobs to Apache Airflow DAGs.
//! This library provides clean architecture-based components for parsing Control-M XML
//! definitions and generating equivalent Airflow DAGs with support for:
//! 
//! - Order date handling (Control-M style)
//! - Various job hold mechanisms
//! - Professional code documentation
//! - Unit testing coverage
//! 
//! ## Architecture
//! 
//! The toolkit follows clean architecture principles with clear separation of concerns:
//! 
//! - **Domain**: Core business logic and entities
//! - **Application**: Use cases and application services  
//! - **Infrastructure**: External integrations (XML parsing, file I/O)
//! - **Presentation**: CLI interface and user interactions
//! 
//! ## Usage
//! 
//! ```rust,ignore
//! use airflow_enterprise_toolkit::{ControlMParser, DagGenerator};
//! 
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let parser = ControlMParser::new();
//! let generator = DagGenerator::new();
//! 
//! let control_m_jobs = parser.parse_xml_str("control_m.xml")?;
//! let airflow_dag = generator.generate_dag(control_m_jobs)?;
//! # Ok(())
//! # }
//! ```

pub mod domain;
pub mod application;
pub mod infrastructure;
pub mod presentation;

// Re-export main components for convenience
pub use domain::entities::*;
pub use application::services::*;
pub use infrastructure::parsers::*;
pub use presentation::cli::*;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_version() {
        assert!(!VERSION.is_empty());
    }
}
