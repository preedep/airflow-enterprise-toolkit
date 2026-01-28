//! Domain layer
//! 
//! This module contains the core business logic and entities that define
//! the domain model for Control-M to Airflow conversion.

pub mod entities;

// Re-export for convenience
pub use entities::*;
