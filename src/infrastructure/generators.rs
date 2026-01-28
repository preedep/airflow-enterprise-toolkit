//! Infrastructure layer for generating Airflow DAG files
//! 
//! This module provides code generation capabilities for creating Airflow DAGs
//! from our domain entities, supporting Python code generation.

use crate::domain::entities::*;
use anyhow::{Context, Result};
use tera::{Context as TeraContext, Tera};
use tracing::debug;

/// Python DAG generator
#[derive(Debug)]
pub struct PythonDagGenerator {
    /// Template engine
    tera: Tera,
}

impl PythonDagGenerator {
    /// Create a new Python DAG generator
    pub fn new() -> Self {
        let mut tera = Tera::default();
        
        // Add built-in templates
        tera.add_raw_template(
            "dag_template",
            include_str!("templates/python_dag.tera"),
        ).expect("Failed to load DAG template");

        tera.add_raw_template(
            "task_template",
            include_str!("templates/python_task.tera"),
        ).expect("Failed to load task template");

        Self { tera }
    }

    /// Generate Python DAG code from Airflow DAG entity
    pub fn generate_python_dag(&self, dag: &AirflowDag) -> Result<String> {
        debug!("Generating Python DAG for: {}", dag.dag_id);

        let mut context = TeraContext::new();
        context.insert("dag", dag);
        context.insert("tasks", &dag.tasks);
        
        // Generate imports
        let imports = self.generate_imports(&dag.tasks);
        context.insert("imports", &imports);

        // Generate task definitions
        let task_definitions: Vec<String> = dag.tasks
            .iter()
            .map(|task| self.generate_task_definition(task))
            .collect::<Result<Vec<_>>>()?;
        context.insert("task_definitions", &task_definitions);

        // Add DAG-level configuration
        let tags_str = format!("{:?}", dag.config.tags);
        context.insert("tags_safe", &tags_str);
        
        let is_paused_str = if dag.config.is_paused { "True" } else { "False" };
        context.insert("is_paused_str", &is_paused_str);

        // Generate dependencies
        let dependencies = self.generate_dependencies(&dag.tasks);
        context.insert("dependencies", &dependencies);

        // Render the template
        let rendered = self.tera
            .render("dag_template", &context)
            .context("Failed to render DAG template")?;

        debug!("Successfully generated Python DAG for: {}", dag.dag_id);
        Ok(rendered)
    }

    /// Generate necessary imports based on task types
    fn generate_imports(&self, tasks: &[AirflowTask]) -> Vec<String> {
        let mut imports = std::collections::HashSet::new();
        
        // Always include base DAG imports
        imports.insert("from datetime import datetime, timedelta".to_string());
        imports.insert("from airflow import DAG".to_string());
        imports.insert("from airflow.operators.dummy import DummyOperator".to_string());

        // Add operator-specific imports
        for task in tasks {
            match task.operator_type.as_str() {
                "BashOperator" => {
                    imports.insert("from airflow.operators.bash import BashOperator".to_string());
                }
                "PythonOperator" => {
                    imports.insert("from airflow.operators.python import PythonOperator".to_string());
                }
                "SqlOperator" => {
                    imports.insert("from airflow.operators.sql import SqlOperator".to_string());
                }
                _ => {
                    // Default to BashOperator for unknown types
                    imports.insert("from airflow.operators.bash import BashOperator".to_string());
                }
            }
        }

        // Add macros if order date handling is used
        if tasks.iter().any(|t| t.order_date_config.is_some()) {
            imports.insert("from airflow.macros import ds_add".to_string());
        }

        let mut import_list: Vec<String> = imports.into_iter().collect();
        import_list.sort();
        import_list
    }

    /// Generate task definition code
    fn generate_task_definition(&self, task: &AirflowTask) -> Result<String> {
        let mut context = TeraContext::new();
        context.insert("task", task);

        // Handle order date configuration
        if let Some(order_date_config) = &task.order_date_config {
            context.insert("order_date_config", order_date_config);
        }

        // Add environment variables if present
        if let Some(env_vars) = &task.environment {
            context.insert("environment", &self.format_env_vars(env_vars));
        }

        // Add execution timeout
        if let Some(timeout) = task.resources.timeout_minutes {
            context.insert("execution_timeout", &(timeout * 60)); // Convert to seconds
        }

        // Render the template
        self.tera
            .render("task_template", &context)
            .context("Failed to render task template")
    }

    /// Format environment variables for Python code
    fn format_env_vars(&self, env_vars: &std::collections::HashMap<String, String>) -> String {
        let mut formatted = "{".to_string();
        let mut first = true;
        
        for (key, value) in env_vars {
            if !first {
                formatted.push_str(", ");
            }
            formatted.push_str(&format!("\"{}\": \"{}\"", key, value));
            first = false;
        }
        
        formatted.push('}');
        formatted
    }

    /// Format task configuration for Python code
    fn format_task_config(&self, config: &std::collections::HashMap<String, serde_json::Value>) -> Result<String> {
        let mut formatted = Vec::new();

        for (key, value) in config {
            let formatted_value = match value {
                serde_json::Value::String(s) => {
                    // Check if it's a template expression
                    if s.contains("{{") && s.contains("}}") {
                        format!("\"{}\"", s)
                    } else {
                        format!("\"{}\"", s)
                    }
                }
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Array(arr) => {
                    let items: Vec<String> = arr
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => format!("\"{}\"", s),
                            _ => v.to_string(),
                        })
                        .collect();
                    format!("[{}]", items.join(", "))
                }
                serde_json::Value::Object(obj) => {
                    let items: Vec<String> = obj
                        .iter()
                        .map(|(k, v)| format!("\"{}\": {}", k, self.format_single_value(v)))
                        .collect();
                    format!("{{{}}}", items.join(", "))
                }
                serde_json::Value::Null => "None".to_string(),
            };

            formatted.push(format!("    {}={}", key, formatted_value));
        }

        Ok(formatted.join(",\n"))
    }

    /// Format a single JSON value for Python
    fn format_single_value(&self, value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => format!("\"{}\"", s),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "None".to_string(),
            _ => value.to_string(),
        }
    }

    /// Generate dependency definitions
    fn generate_dependencies(&self, tasks: &[AirflowTask]) -> Vec<String> {
        let mut dependencies = Vec::new();

        for task in tasks {
            if !task.dependencies.is_empty() {
                let dep_list = task.dependencies.join(", ");
                dependencies.push(format!(
                    "    {}.set_upstream([{}])",
                    task.task_id,
                    dep_list
                ));
            }
        }

        dependencies
    }
}

impl Default for PythonDagGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON DAG generator for debugging and analysis
#[derive(Debug)]
pub struct JsonDagGenerator;

impl JsonDagGenerator {
    /// Create a new JSON DAG generator
    pub fn new() -> Self {
        Self
    }

    /// Generate JSON representation of Airflow DAG
    pub fn generate_json_dag(&self, dag: &AirflowDag) -> Result<String> {
        serde_json::to_string_pretty(dag)
            .context("Failed to serialize DAG to JSON")
    }
}

impl Default for JsonDagGenerator {
    fn default() -> Self {
        Self::new()
    }
}
