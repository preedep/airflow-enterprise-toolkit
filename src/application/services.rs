//! Application services for Control-M to Airflow conversion
//! 
//! This module contains the use cases and application services that orchestrate
//! the conversion process from Control-M jobs to Airflow DAGs.

use crate::domain::entities::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Service for converting Control-M jobs to Airflow DAGs
#[derive(Debug)]
pub struct DagGenerator {
    /// Configuration for DAG generation
    config: GenerationConfig,
}

/// Configuration for DAG generation
#[derive(Debug, Clone)]
pub struct GenerationConfig {
    /// Default timezone for DAGs
    pub default_timezone: String,
    
    /// Default start date for DAGs
    pub default_start_date: DateTime<Utc>,
    
    /// Whether to include order date handling
    pub include_order_date: bool,
    
    /// Template for order date handling
    pub order_date_template: String,
    
    /// Default tags for generated DAGs
    pub default_tags: Vec<String>,
}

impl DagGenerator {
    /// Create a new DAG generator with default configuration
    pub fn new() -> Self {
        Self::with_config(GenerationConfig::default())
    }

    /// Create a new DAG generator with custom configuration
    pub fn with_config(config: GenerationConfig) -> Self {
        Self { config }
    }

    /// Generate an Airflow DAG from Control-M jobs
    pub fn generate_dag(&self, jobs: Vec<ControlMJob>) -> Result<AirflowDag> {
        info!("Generating Airflow DAG from {} Control-M jobs", jobs.len());
        
        if jobs.is_empty() {
            return Err(anyhow::anyhow!("No jobs provided for DAG generation"));
        }

        let dag_id = self.generate_dag_id(&jobs);
        let description = self.generate_dag_description(&jobs);
        let schedule_interval = self.determine_schedule_interval(&jobs);
        
        let tasks = jobs
            .into_iter()
            .map(|job| self.convert_job_to_task(job))
            .collect::<Result<Vec<_>>>()?;

        let dag = AirflowDag {
            dag_id,
            description,
            schedule_interval,
            start_date: self.config.default_start_date,
            timezone: self.config.default_timezone.clone(),
            tasks,
            config: DagConfig {
                max_active_runs: Some(1),
                is_paused: false,
                tags: self.config.default_tags.clone(),
                default_args: HashMap::new(),
            },
        };

        info!("Successfully generated DAG: {}", dag.dag_id);
        Ok(dag)
    }

    /// Convert a single Control-M job to an Airflow task
    pub fn convert_job_to_task(&self, job: ControlMJob) -> Result<AirflowTask> {
        debug!("Converting Control-M job '{}' to Airflow task", job.name);

        let task_id = self.sanitize_task_id(&job.name);
        let operator_type = self.determine_operator_type(&job.job_type);
        let config = self.build_task_config(&job)?;
        let dependencies = self.extract_dependencies(&job);
        let order_date_config = if self.config.include_order_date {
            Some(self.build_order_date_config(&job.schedule.order_date))
        } else {
            None
        };

        Ok(AirflowTask {
            task_id,
            operator_type,
            config,
            dependencies,
            order_date_config,
        })
    }

    /// Generate a DAG ID from multiple jobs
    fn generate_dag_id(&self, jobs: &[ControlMJob]) -> String {
        if jobs.len() == 1 {
            format!("control_m_{}", self.sanitize_task_id(&jobs[0].name))
        } else {
            // Use the first job name as base, add suffix for multiple jobs
            format!(
                "control_m_group_{}",
                self.sanitize_task_id(&jobs[0].name)
            )
        }
    }

    /// Generate a DAG description from jobs
    fn generate_dag_description(&self, jobs: &[ControlMJob]) -> Option<String> {
        if jobs.len() == 1 {
            jobs[0].description.clone()
        } else {
            Some(format!(
                "Control-M migration DAG containing {} jobs",
                jobs.len()
            ))
        }
    }

    /// Determine the schedule interval for the DAG
    fn determine_schedule_interval(&self, jobs: &[ControlMJob]) -> String {
        // For now, use the first job's schedule. In a more complex scenario,
        // we might need to reconcile different schedules
        match &jobs[0].schedule.frequency {
            ScheduleFrequency::Once => "@once".to_string(),
            ScheduleFrequency::Daily => "@daily".to_string(),
            ScheduleFrequency::Weekly => "@weekly".to_string(),
            ScheduleFrequency::Monthly => "@monthly".to_string(),
            ScheduleFrequency::Custom(cron) => cron.clone(),
        }
    }

    /// Sanitize job name to create valid Airflow task ID
    fn sanitize_task_id(&self, name: &str) -> String {
        name.to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect::<String>()
            .trim_matches('_')
            .to_string()
    }

    /// Determine the appropriate Airflow operator type
    fn determine_operator_type(&self, job_type: &JobType) -> String {
        match job_type {
            JobType::Command => "BashOperator".to_string(),
            JobType::Script { interpreter } => {
                match interpreter.to_lowercase().as_str() {
                    "python" => "PythonOperator".to_string(),
                    "bash" | "sh" => "BashOperator".to_string(),
                    _ => "BashOperator".to_string(),
                }
            }
            JobType::Database { .. } => "SqlOperator".to_string(),
            JobType::FileTransfer => "BashOperator".to_string(), // Simplified for now
            JobType::Custom(_) => "BashOperator".to_string(), // Default fallback
        }
    }

    /// Build task configuration based on Control-M job
    fn build_task_config(&self, job: &ControlMJob) -> Result<HashMap<String, serde_json::Value>> {
        let mut config = HashMap::new();

        match &job.job_type {
            JobType::Command => {
                config.insert("bash_command".to_string(), 
                    serde_json::Value::String(job.command.clone()));
            }
            JobType::Script { interpreter } => {
                let script_content = if interpreter.to_lowercase() == "python" {
                    format!("python3 -c \"{}\"", job.command)
                } else {
                    job.command.clone()
                };
                config.insert("bash_command".to_string(), 
                    serde_json::Value::String(script_content));
            }
            JobType::Database { .. } => {
                // Database jobs would need more complex handling
                config.insert("sql".to_string(), 
                    serde_json::Value::String(job.command.clone()));
            }
            _ => {
                config.insert("bash_command".to_string(), 
                    serde_json::Value::String(job.command.clone()));
            }
        }

        // Add environment variables
        if !job.environment.is_empty() {
            config.insert("env".to_string(), 
                serde_json::to_value(&job.environment)?);
        }

        // Add resource requirements
        if let Some(timeout) = job.resources.timeout_minutes {
            config.insert("execution_timeout".to_string(), 
                serde_json::Value::Number(serde_json::Number::from(timeout * 60))); // Convert to seconds
        }

        Ok(config)
    }

    /// Extract dependencies from Control-M job
    fn extract_dependencies(&self, job: &ControlMJob) -> Vec<String> {
        job.dependencies
            .iter()
            .map(|dep| self.sanitize_task_id(&dep.job_name))
            .collect()
    }

    /// Build order date configuration
    fn build_order_date_config(&self, order_date: &OrderDateHandling) -> OrderDateConfig {
        let template = match order_date {
            OrderDateHandling::Current => "{{ ds }}".to_string(),
            OrderDateHandling::PreviousBusinessDay => "{{ macros.ds_add(ds, -1) }}".to_string(),
            OrderDateHandling::DayOffset(offset) => {
                format!("{{{{ macros.ds_add(ds, {}) }}}}", offset)
            }
            OrderDateHandling::Custom(template) => template.clone(),
        };

        OrderDateConfig {
            strategy: order_date.clone(),
            template,
            as_parameter: true,
        }
    }
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            default_timezone: "UTC".to_string(),
            default_start_date: Utc::now(),
            include_order_date: true,
            order_date_template: "{{ ds }}".to_string(),
            default_tags: vec!["control-m-migration".to_string()],
        }
    }
}

/// Service for validating Control-M jobs
#[derive(Debug)]
pub struct JobValidator;

impl JobValidator {
    /// Validate a Control-M job
    pub fn validate_job(job: &ControlMJob) -> Result<()> {
        if job.name.is_empty() {
            return Err(anyhow::anyhow!("Job name cannot be empty"));
        }

        if job.command.is_empty() {
            return Err(anyhow::anyhow!("Job command cannot be empty"));
        }

        // Validate cron expression if present
        if let Some(cron) = &job.schedule.cron_expression {
            // Basic validation - in a real implementation, we'd use a cron parser
            if cron.split_whitespace().count() != 5 {
                warn!("Cron expression '{}' may not be valid", cron);
            }
        }

        // Validate dependencies
        for dep in &job.dependencies {
            if dep.job_name.is_empty() {
                return Err(anyhow::anyhow!("Dependency job name cannot be empty"));
            }
        }

        debug!("Job '{}' validation passed", job.name);
        Ok(())
    }

    /// Validate multiple jobs and check for circular dependencies
    pub fn validate_jobs(jobs: &[ControlMJob]) -> Result<()> {
        for job in jobs {
            Self::validate_job(job)?;
        }

        // Check for circular dependencies
        if let Some(cycle) = Self::detect_circular_dependencies(jobs) {
            return Err(anyhow::anyhow!(
                "Circular dependency detected: {}",
                cycle.join(" -> ")
            ));
        }

        Ok(())
    }

    /// Detect circular dependencies using DFS
    fn detect_circular_dependencies(jobs: &[ControlMJob]) -> Option<Vec<String>> {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();
        let mut path = Vec::new();

        for job in jobs {
            if !visited.contains(&job.name) {
                if let Some(cycle) = Self::dfs_cycle_check(
                    &job.name,
                    jobs,
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                ) {
                    return Some(cycle);
                }
            }
        }

        None
    }

    /// DFS helper for cycle detection
    fn dfs_cycle_check(
        job_name: &str,
        jobs: &[ControlMJob],
        visited: &mut std::collections::HashSet<String>,
        rec_stack: &mut std::collections::HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(job_name.to_string());
        rec_stack.insert(job_name.to_string());
        path.push(job_name.to_string());

        if let Some(job) = jobs.iter().find(|j| j.name == job_name) {
            for dep in &job.dependencies {
                if !visited.contains(&dep.job_name) {
                    if let Some(cycle) = Self::dfs_cycle_check(
                        &dep.job_name,
                        jobs,
                        visited,
                        rec_stack,
                        path,
                    ) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&dep.job_name) {
                    // Found a cycle
                    let cycle_start = path.iter().position(|n| n == &dep.job_name).unwrap();
                    return Some(path[cycle_start..].to_vec());
                }
            }
        }

        rec_stack.remove(job_name);
        path.pop();
        None
    }
}
