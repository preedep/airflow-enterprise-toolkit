//! Domain entities for Control-M and Airflow concepts
//! 
//! This module contains the core business entities that represent
//! Control-M jobs and their Airflow equivalents.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents a Control-M job with all its properties
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlMJob {
    /// Unique identifier for the job
    pub id: Uuid,
    
    /// Job name as defined in Control-M
    pub name: String,
    
    /// Job description
    pub description: Option<String>,
    
    /// Command or script to execute
    pub command: String,
    
    /// Job type (e.g., "Command", "Script", "Database")
    pub job_type: JobType,
    
    /// Scheduling information
    pub schedule: ScheduleInfo,
    
    /// Dependencies on other jobs
    pub dependencies: Vec<JobDependency>,
    
    /// Hold conditions
    pub hold_conditions: Vec<HoldCondition>,
    
    /// Environment variables
    pub environment: HashMap<String, String>,
    
    /// Resource requirements
    pub resources: ResourceRequirements,
    
    /// Creation and modification timestamps
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Types of Control-M jobs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobType {
    /// Simple command execution
    Command,
    /// Script execution (Shell, Python, etc.)
    Script { interpreter: String },
    /// Database job
    Database { db_type: String },
    /// File transfer job
    FileTransfer,
    /// Custom job type
    Custom(String),
}

/// Scheduling information for jobs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleInfo {
    /// Order date handling (Control-M specific)
    pub order_date: OrderDateHandling,
    
    /// Cron-like schedule
    pub cron_expression: Option<String>,
    
    /// Run frequency
    pub frequency: ScheduleFrequency,
    
    /// Time zone
    pub timezone: String,
    
    /// Start and end dates
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
}

/// Order date handling strategies (Control-M feature)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderDateHandling {
    /// Use current date as order date
    Current,
    /// Use previous business day as order date
    PreviousBusinessDay,
    /// Use specific offset from current date
    DayOffset(i32),
    /// Use custom order date logic
    Custom(String),
}

/// Job execution frequency
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScheduleFrequency {
    /// Run once
    Once,
    /// Daily
    Daily,
    /// Weekly
    Weekly,
    /// Monthly
    Monthly,
    /// Custom cron expression
    Custom(String),
}

/// Job dependency information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobDependency {
    /// Name of the dependent job
    pub job_name: String,
    
    /// Type of dependency
    pub dependency_type: DependencyType,
    
    /// Condition for dependency satisfaction
    pub condition: Option<String>,
}

/// Types of job dependencies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DependencyType {
    /// Simple success dependency
    Success,
    /// Failure dependency
    Failure,
    /// Completion dependency (success or failure)
    Completion,
    /// Custom condition
    Custom(String),
}

/// Hold conditions for jobs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HoldCondition {
    /// Type of hold condition
    pub condition_type: HoldType,
    
    /// Condition value or expression
    pub value: String,
    
    /// Whether this hold is active
    pub active: bool,
}

/// Types of hold conditions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HoldType {
    /// Time-based hold
    TimeHold,
    /// File-based hold
    FileHold,
    /// Resource-based hold
    ResourceHold,
    /// Manual hold
    ManualHold,
    /// Custom hold condition
    CustomHold(String),
}

/// Resource requirements for job execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceRequirements {
    /// Memory requirement in MB
    pub memory_mb: Option<u32>,
    
    /// CPU requirement
    pub cpu_cores: Option<u32>,
    
    /// Disk space requirement in MB
    pub disk_mb: Option<u32>,
    
    /// Maximum execution time in minutes
    pub timeout_minutes: Option<u32>,
}

/// Represents an Airflow DAG generated from Control-M jobs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirflowDag {
    /// DAG identifier
    pub dag_id: String,
    
    /// DAG description
    pub description: Option<String>,
    
    /// Schedule interval
    pub schedule_interval: String,
    
    /// Start date
    pub start_date: DateTime<Utc>,
    
    /// Time zone
    pub timezone: String,
    
    /// List of tasks in the DAG
    pub tasks: Vec<AirflowTask>,
    
    /// DAG-level configuration
    pub config: DagConfig,
}

/// Represents an Airflow task
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirflowTask {
    /// Task identifier
    pub task_id: String,
    
    /// Task type (e.g., BashOperator, PythonOperator)
    pub operator_type: String,
    
    /// Task configuration parameters
    pub config: HashMap<String, serde_json::Value>,
    
    /// Dependencies on other tasks
    pub dependencies: Vec<String>,
    
    /// Order date handling configuration
    pub order_date_config: Option<OrderDateConfig>,
}

/// Order date configuration for Airflow tasks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderDateConfig {
    /// Order date handling strategy
    pub strategy: OrderDateHandling,
    
    /// Template for order date in task execution
    pub template: String,
    
    /// Whether to pass order date as parameter
    pub as_parameter: bool,
}

/// DAG-level configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DagConfig {
    /// Maximum active runs
    pub max_active_runs: Option<u32>,
    
    /// Whether DAG is paused
    pub is_paused: bool,
    
    /// Tags for the DAG
    pub tags: Vec<String>,
    
    /// Default arguments for tasks
    pub default_args: HashMap<String, serde_json::Value>,
}

impl ControlMJob {
    /// Create a new Control-M job
    pub fn new(name: String, command: String, job_type: JobType) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            description: None,
            command,
            job_type,
            schedule: ScheduleInfo::default(),
            dependencies: Vec::new(),
            hold_conditions: Vec::new(),
            environment: HashMap::new(),
            resources: ResourceRequirements::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a dependency to this job
    pub fn add_dependency(&mut self, job_name: String, dependency_type: DependencyType) {
        self.dependencies.push(JobDependency {
            job_name,
            dependency_type,
            condition: None,
        });
        self.updated_at = Utc::now();
    }

    /// Add a hold condition to this job
    pub fn add_hold_condition(&mut self, condition_type: HoldType, value: String) {
        self.hold_conditions.push(HoldCondition {
            condition_type,
            value,
            active: true,
        });
        self.updated_at = Utc::now();
    }

    /// Add an environment variable
    pub fn add_environment_variable(&mut self, key: String, value: String) {
        self.environment.insert(key, value);
        self.updated_at = Utc::now();
    }
}

impl Default for ScheduleInfo {
    fn default() -> Self {
        Self {
            order_date: OrderDateHandling::Current,
            cron_expression: None,
            frequency: ScheduleFrequency::Once,
            timezone: "UTC".to_string(),
            valid_from: None,
            valid_to: None,
        }
    }
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            memory_mb: None,
            cpu_cores: None,
            disk_mb: None,
            timeout_minutes: None,
        }
    }
}

impl Default for DagConfig {
    fn default() -> Self {
        Self {
            max_active_runs: Some(1),
            is_paused: false,
            tags: vec!["control-m-migration".to_string()],
            default_args: HashMap::new(),
        }
    }
}
