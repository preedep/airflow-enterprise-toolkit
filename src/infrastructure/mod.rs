//! Infrastructure layer
//! 
//! This module contains the external integrations and technical implementations
//! including XML parsing, file I/O, and other infrastructure concerns.

pub mod parsers;
pub mod generators;

// Re-export for convenience
pub use parsers::*;
pub use generators::*;
