//! Infrastructure layer for parsing Control-M XML files
//! 
//! This module provides XML parsing capabilities for Control-M job definitions,
//! handling the conversion from XML format to our domain entities.

use crate::domain::entities::*;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use quick_xml::de::from_str;

/// Control-M XML structure for deserialization
#[derive(Debug, Deserialize, Serialize)]
pub struct ControlMXml {
    #[serde(rename = "DEFTABLE")]
    pub def_table: ControlMDefTable,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ControlMDefTable {
    #[serde(rename = "FOLDER", default)]
    pub folders: Vec<ControlMFolder>,
    #[serde(rename = "SMART_FOLDER", default)]
    pub smart_folders: Vec<ControlMSmartFolder>,
    #[serde(rename = "SCHED_TABLE", default)]
    pub sched_tables: Vec<ControlMFolder>,
    #[serde(rename = "TABLE", default)]
    pub tables: Vec<ControlMFolder>,
    #[serde(default)]
    pub workspace: Option<WorkspaceData>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkspaceData {
    #[serde(rename = "WORKSPACE")]
    pub workspace: Option<Workspace>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Workspace {
    #[serde(rename = "NAME")]
    pub name: Option<String>,
    #[serde(rename = "DESCRIPTION")]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ControlMFolder {
    #[serde(rename = "FOLDER_NAME")]
    pub name: String,
    #[serde(rename = "DESCRIPTION", default)]
    pub description: Option<String>,
    #[serde(rename = "JOB", default)]
    pub jobs: Vec<ControlMJobXml>,
    #[serde(rename = "DATACENTER", default)]
    pub datacenter: Option<String>,
    #[serde(rename = "VERSION", default)]
    pub version: Option<String>,
    #[serde(rename = "PLATFORM", default)]
    pub platform: Option<String>,
    #[serde(rename = "TABLE_NAME", default)]
    pub table_name: Option<String>,
    #[serde(rename = "FOLDER_ORDER_METHOD", default)]
    pub order_method: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ControlMSmartFolder {
    #[serde(rename = "FOLDER_NAME")]
    pub name: String,
    #[serde(rename = "DESCRIPTION", default)]
    pub description: Option<String>,
    #[serde(rename = "SUB_FOLDER", default)]
    pub sub_folders: Vec<ControlMSubFolder>,
    #[serde(rename = "JOB", default)]
    pub jobs: Vec<ControlMJobXml>,
    #[serde(rename = "DATACENTER", default)]
    pub datacenter: Option<String>,
    #[serde(rename = "VERSION", default)]
    pub version: Option<String>,
    #[serde(rename = "PLATFORM", default)]
    pub platform: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMSubFolder {
    #[serde(rename = "FOLDER_NAME")]
    pub name: String,
    #[serde(rename = "JOB", default)]
    pub jobs: Vec<ControlMJobXml>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMJobXml {
    #[serde(rename = "JOBNAME")]
    pub name: String,
    #[serde(rename = "DESCRIPTION", default)]
    pub description: Option<String>,
    #[serde(rename = "CMDLINE", default = "default_command")]
    pub command: String,
    #[serde(rename = "TASKTYPE", default = "default_job_type")]
    pub job_type: String,
    #[serde(rename = "APPLICATION", default)]
    pub application: Option<String>,
    #[serde(rename = "SUB_APPLICATION", default)]
    pub sub_application: Option<String>,
    #[serde(rename = "GROUP", default)]
    pub group: Option<String>,
    #[serde(rename = "OWNER", default)]
    pub owner: Option<String>,
    #[serde(rename = "CREATED_BY", default)]
    pub created_by: Option<String>,
    #[serde(rename = "AUTHOR", default)]
    pub author: Option<String>,
    #[serde(rename = "RUN_AS", default)]
    pub run_as: Option<String>,
    #[serde(rename = "PRIORITY", default)]
    pub priority: Option<String>,
    #[serde(rename = "CRITICAL", default)]
    pub critical: Option<String>,
    #[serde(rename = "NODEID", default)]
    pub node_id: Option<String>,
    #[serde(rename = "TIMEZONE", default)]
    pub timezone: Option<String>,
    #[serde(rename = "CREATION_DATE", default)]
    pub creation_date: Option<String>,
    #[serde(rename = "CREATION_TIME", default)]
    pub creation_time: Option<String>,
    #[serde(rename = "CHANGE_DATE", default)]
    pub change_date: Option<String>,
    #[serde(rename = "CHANGE_TIME", default)]
    pub change_time: Option<String>,
    #[serde(rename = "MAXWAIT", default)]
    pub max_wait: Option<i32>,
    #[serde(rename = "MAXRERUN", default)]
    pub max_rerun: Option<i32>,
    #[serde(rename = "MAXDAYS", default)]
    pub max_days: Option<i32>,
    #[serde(rename = "MAXRUNS", default)]
    pub max_runs: Option<i32>,
    #[serde(rename = "TIMEFROM", default)]
    pub time_from: Option<String>,
    #[serde(rename = "TIMETO", default)]
    pub time_to: Option<String>,
    #[serde(rename = "DAYS", default)]
    pub days: Option<String>,
    #[serde(rename = "WEEKDAYS", default)]
    pub weekdays: Option<String>,
    #[serde(rename = "MONTHS", default)]
    pub months: Option<String>,
    #[serde(rename = "DATES", default)]
    pub dates: Option<String>,
    #[serde(rename = "CYCLIC", default)]
    pub cyclic: Option<String>,
    #[serde(rename = "INTERVAL", default)]
    pub interval: Option<String>,
    #[serde(rename = "INCOND", default)]
    pub in_conditions: Vec<ControlMInConditionXml>,
    #[serde(rename = "OUTCOND", default)]
    pub out_conditions: Vec<ControlMOutConditionXml>,
    #[serde(rename = "VARIABLE", default)]
    pub variables: Vec<ControlMVariableXml>,
    #[serde(rename = "AUTOEDIT2", default)]
    pub autoedit2: Vec<ControlMVariableXml>,
    #[serde(rename = "SHOUT", default)]
    pub shouts: Vec<ControlMShoutXml>,
    #[serde(rename = "CONTROL", default)]
    pub control_resources: Vec<ControlMControlResourceXml>,
    #[serde(rename = "QUANTITATIVE", default)]
    pub quantitative_resources: Vec<ControlMQuantitativeResourceXml>,
    #[serde(rename = "ON", default)]
    pub on_actions: Vec<ControlMOnActionXml>,
    #[serde(rename = "CAPTURE", default)]
    pub captures: Vec<ControlMCaptureXml>,
}

fn default_command() -> String {
    "".to_string()
}

fn default_job_type() -> String {
    "Command".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMInConditionXml {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "OP", default = "default_operator")]
    pub operator: String,
    #[serde(rename = "ODATE", default)]
    pub odate: Option<String>,
    #[serde(rename = "AND_OR", default)]
    pub and_or: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMOutConditionXml {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "SIGN", default = "default_sign")]
    pub sign: String,
    #[serde(rename = "ODATE", default)]
    pub odate: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMVariableXml {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "VALUE")]
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMShoutXml {
    #[serde(rename = "WHEN")]
    pub when: Option<String>,
    #[serde(rename = "TIME")]
    pub time: Option<String>,
    #[serde(rename = "URGENCY")]
    pub urgency: Option<String>,
    #[serde(rename = "DEST")]
    pub destination: Option<String>,
    #[serde(rename = "MESSAGE")]
    pub message: Option<String>,
    #[serde(rename = "DAYSOFFSET")]
    pub days_offset: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMControlResourceXml {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "TYPE")]
    pub resource_type: Option<String>,
    #[serde(rename = "ONFAIL")]
    pub on_fail: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMQuantitativeResourceXml {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "QUANT")]
    pub quantity: Option<i32>,
    #[serde(rename = "ONFAIL")]
    pub on_fail: Option<String>,
    #[serde(rename = "ONOK")]
    pub on_ok: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMOnActionXml {
    #[serde(rename = "DO")]
    pub actions: Vec<ControlMDoActionXml>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMDoActionXml {
    #[serde(rename = "COMMAND")]
    pub command: Option<String>,
    #[serde(rename = "COND")]
    pub condition: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ControlMCaptureXml {
    #[serde(rename = "FROM")]
    pub from: Option<String>,
    #[serde(rename = "TO")]
    pub to: Option<String>,
    #[serde(rename = "COND")]
    pub condition: Option<String>,
}

fn default_operator() -> String {
    "EQ".to_string()
}

fn default_sign() -> String {
    "+".to_string()
}

/// Parser for Control-M XML files
#[derive(Debug)]
pub struct ControlMParser {
    /// Parser configuration
    config: ParserConfig,
}

/// Configuration for XML parsing
#[derive(Debug, Clone)]
pub struct ParserConfig {
    /// Default timezone for parsed jobs
    pub default_timezone: String,
    
    /// Whether to validate XML structure strictly
    pub strict_validation: bool,
    
    /// Default job timeout in minutes
    pub default_timeout_minutes: u32,
}

impl ControlMParser {
    /// Create a new parser with default configuration
    pub fn new() -> Self {
        Self::with_config(ParserConfig::default())
    }

    /// Create a new parser with custom configuration
    pub fn with_config(config: ParserConfig) -> Self {
        Self { config }
    }

    /// Parse Control-M XML from string content
    pub fn parse_xml_str(&self, xml_content: &str) -> Result<Vec<ControlMJob>> {
        info!("Parsing Control-M XML content");
        debug!("XML content length: {} characters", xml_content.len());

        let control_m_xml: ControlMXml = from_str(xml_content)
            .context("Failed to parse Control-M XML")?;

        let mut all_jobs = Vec::new();

        // Process regular folders
        for folder in &control_m_xml.def_table.folders {
            debug!("Processing folder: {}", folder.name);
            let mut jobs = self.process_folder_jobs(&folder.jobs, folder)?;
            all_jobs.append(&mut jobs);
        }

        // Process smart folders
        for smart_folder in &control_m_xml.def_table.smart_folders {
            debug!("Processing smart folder: {}", smart_folder.name);
            let mut jobs = self.process_folder_jobs(&smart_folder.jobs, &ControlMFolder {
                name: smart_folder.name.clone(),
                description: smart_folder.description.clone(),
                jobs: smart_folder.jobs.clone(),
                datacenter: smart_folder.datacenter.clone(),
                version: smart_folder.version.clone(),
                platform: smart_folder.platform.clone(),
                table_name: None,
                order_method: None,
            })?;
            all_jobs.append(&mut jobs);

            // Process sub-folders in smart folders
            for sub_folder in &smart_folder.sub_folders {
                debug!("Processing sub-folder: {}", sub_folder.name);
                let mut jobs = self.process_folder_jobs(&sub_folder.jobs, &ControlMFolder {
                    name: format!("{}_{}", smart_folder.name, sub_folder.name),
                    description: None,
                    jobs: sub_folder.jobs.clone(),
                    datacenter: smart_folder.datacenter.clone(),
                    version: smart_folder.version.clone(),
                    platform: smart_folder.platform.clone(),
                    table_name: None,
                    order_method: None,
                })?;
                all_jobs.append(&mut jobs);
            }
        }

        // Process scheduled tables
        for sched_table in &control_m_xml.def_table.sched_tables {
            debug!("Processing scheduled table: {}", sched_table.name);
            let mut jobs = self.process_folder_jobs(&sched_table.jobs, sched_table)?;
            all_jobs.append(&mut jobs);
        }

        // Process regular tables
        for table in &control_m_xml.def_table.tables {
            debug!("Processing table: {}", table.name);
            let mut jobs = self.process_folder_jobs(&table.jobs, table)?;
            all_jobs.append(&mut jobs);
        }

        info!("Successfully parsed {} Control-M jobs", all_jobs.len());
        Ok(all_jobs)
    }

    /// Process jobs from a folder
    fn process_folder_jobs(&self, jobs: &[ControlMJobXml], folder: &ControlMFolder) -> Result<Vec<ControlMJob>> {
        let mut processed_jobs = Vec::new();
        
        for job_xml in jobs {
            match self.convert_xml_job_to_entity(job_xml, folder) {
                Ok(job) => {
                    debug!("Successfully parsed job: {}", job.name);
                    processed_jobs.push(job);
                }
                Err(e) => {
                    warn!("Failed to parse job '{}': {}", job_xml.name, e);
                    if self.config.strict_validation {
                        return Err(e);
                    }
                }
            }
        }
        
        Ok(processed_jobs)
    }

    /// Parse Control-M XML from file
    pub fn parse_xml_file(&self, file_path: &str) -> Result<Vec<ControlMJob>> {
        info!("Parsing Control-M XML file: {}", file_path);
        
        let xml_content = std::fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read file: {}", file_path))?;

        self.parse_xml_str(&xml_content)
    }

    /// Convert XML job to domain entity
    fn convert_xml_job_to_entity(
        &self,
        job_xml: &ControlMJobXml,
        folder: &ControlMFolder,
    ) -> Result<ControlMJob> {
        let job_type = self.parse_job_type(&job_xml.job_type);
        let schedule = self.parse_schedule(job_xml)?;
        let dependencies = self.parse_dependencies(&job_xml.in_conditions)?;
        let hold_conditions = self.parse_hold_conditions(&job_xml.in_conditions)?;
        let environment = self.parse_variables(&job_xml.variables, &job_xml.autoedit2)?;
        let resources = self.parse_resources(job_xml)?;

        let now = chrono::Utc::now();
        
        Ok(ControlMJob {
            id: uuid::Uuid::new_v4(),
            name: job_xml.name.clone(),
            description: job_xml.description.clone().or_else(|| folder.description.clone()),
            command: job_xml.command.clone(),
            job_type,
            schedule,
            dependencies,
            hold_conditions,
            environment,
            resources,
            created_at: now,
            updated_at: now,
        })
    }

    /// Parse job type from XML string
    fn parse_job_type(&self, job_type_str: &str) -> JobType {
        match job_type_str.to_lowercase().as_str() {
            "command" | "job" => JobType::Command,
            "script" => JobType::Script {
                interpreter: "bash".to_string(),
            },
            "python" => JobType::Script {
                interpreter: "python".to_string(),
            },
            "database" | "db" => JobType::Database {
                db_type: "generic".to_string(),
            },
            "file" | "transfer" => JobType::FileTransfer,
            _ => JobType::Custom(job_type_str.to_string()),
        }
    }

    /// Parse schedule information
    fn parse_schedule(&self, job_xml: &ControlMJobXml) -> Result<ScheduleInfo> {
        let order_date = self.parse_order_date_from_job(job_xml)?;
        let frequency = self.parse_frequency(job_xml);
        let cron_expression = self.build_cron_expression(job_xml);

        Ok(ScheduleInfo {
            order_date,
            cron_expression,
            frequency,
            timezone: job_xml.timezone.clone().unwrap_or_else(|| "UTC".to_string()),
            valid_from: None,
            valid_to: None,
        })
    }

    /// Parse order date from job attributes
    fn parse_order_date_from_job(&self, job_xml: &ControlMJobXml) -> Result<OrderDateHandling> {
        // Check for order date in various fields
        if let Some(days) = &job_xml.days {
            if days.contains("ODATE") || days.contains("ORDERDATE") {
                return Ok(OrderDateHandling::Current);
            }
        }
        
        // Check for previous day patterns
        if let Some(weekdays) = &job_xml.weekdays {
            if weekdays.contains("PREV") || weekdays.contains("PREVIOUS") {
                return Ok(OrderDateHandling::PreviousBusinessDay);
            }
        }

        Ok(OrderDateHandling::Current)
    }

    /// Parse frequency from job attributes
    fn parse_frequency(&self, job_xml: &ControlMJobXml) -> ScheduleFrequency {
        if let Some(days) = &job_xml.days {
            match days.to_lowercase().as_str() {
                "daily" | "everyday" | "all" => ScheduleFrequency::Daily,
                "weekly" => ScheduleFrequency::Weekly,
                "monthly" => ScheduleFrequency::Monthly,
                _ => ScheduleFrequency::Custom(days.clone()),
            }
        } else if job_xml.cyclic.as_ref().map_or(false, |c| c == "1") {
            ScheduleFrequency::Custom("cyclic".to_string())
        } else {
            ScheduleFrequency::Once
        }
    }

    /// Build cron expression from job attributes
    fn build_cron_expression(&self, job_xml: &ControlMJobXml) -> Option<String> {
        let time_part = match (&job_xml.time_from, &job_xml.time_to) {
            (Some(from), Some(to)) => format!("{}-{}", from, to),
            (Some(time), None) => time.clone(),
            (None, Some(time)) => time.clone(),
            (None, None) => "0".to_string(),
        };

        let day_part = job_xml.days.as_ref().map_or("*", |d| d.as_str());
        let month_part = job_xml.months.as_ref().map_or("*", |m| m.as_str());
        
        Some(format!("{} {} {} * {}", time_part, "*", day_part, month_part))
    }

    /// Parse dependencies from in-conditions
    fn parse_dependencies(&self, in_conditions: &[ControlMInConditionXml]) -> Result<Vec<JobDependency>> {
        let mut dependencies = Vec::new();

        for condition in in_conditions {
            // Only treat as dependency if it's not a hold condition
            if !self.is_hold_condition(&condition.name) {
                let dependency_type = self.parse_dependency_operator(&condition.operator);
                
                dependencies.push(JobDependency {
                    job_name: condition.name.clone(),
                    dependency_type,
                    condition: condition.odate.clone(),
                });
            }
        }

        Ok(dependencies)
    }

    /// Parse hold conditions from in-conditions
    fn parse_hold_conditions(&self, in_conditions: &[ControlMInConditionXml]) -> Result<Vec<HoldCondition>> {
        let mut hold_conditions = Vec::new();

        for condition in in_conditions {
            if self.is_hold_condition(&condition.name) {
                let hold_type = self.parse_hold_type(&condition.name);
                
                hold_conditions.push(HoldCondition {
                    condition_type: hold_type,
                    value: condition.odate.clone().unwrap_or_default(),
                    active: true,
                });
            }
        }

        Ok(hold_conditions)
    }

    /// Check if condition is a hold condition
    fn is_hold_condition(&self, condition_name: &str) -> bool {
        condition_name.to_lowercase().contains("hold") 
            || condition_name.to_lowercase().contains("wait")
            || condition_name.to_lowercase().contains("file")
    }

    /// Parse dependency operator
    fn parse_dependency_operator(&self, operator: &str) -> DependencyType {
        match operator.to_uppercase().as_str() {
            "EQ" => DependencyType::Success,
            "NE" => DependencyType::Failure,
            "GT" | "LT" | "GE" | "LE" => DependencyType::Custom(operator.to_string()),
            _ => DependencyType::Success,
        }
    }

    /// Parse hold type
    fn parse_hold_type(&self, condition_name: &str) -> HoldType {
        let name_lower = condition_name.to_lowercase();
        
        if name_lower.contains("time") {
            HoldType::TimeHold
        } else if name_lower.contains("file") {
            HoldType::FileHold
        } else if name_lower.contains("resource") {
            HoldType::ResourceHold
        } else if name_lower.contains("manual") {
            HoldType::ManualHold
        } else {
            HoldType::CustomHold(condition_name.to_string())
        }
    }

    /// Parse environment variables
    fn parse_variables(
        &self, 
        variables: &[ControlMVariableXml], 
        autoedit2: &[ControlMVariableXml]
    ) -> Result<HashMap<String, String>> {
        let mut env_vars = HashMap::new();

        // Process regular variables
        for variable in variables {
            env_vars.insert(variable.name.clone(), variable.value.clone());
        }

        // Process AUTOEDIT2 variables (these are typically runtime variables)
        for variable in autoedit2 {
            env_vars.insert(variable.name.clone(), variable.value.clone());
        }

        Ok(env_vars)
    }

    /// Parse resource requirements
    fn parse_resources(&self, job_xml: &ControlMJobXml) -> Result<ResourceRequirements> {
        let timeout_minutes = job_xml.max_wait
            .or_else(|| job_xml.max_rerun)
            .or_else(|| job_xml.max_days)
            .or_else(|| job_xml.max_runs)
            .map(|t| t as u32)
            .or_else(|| Some(self.config.default_timeout_minutes));

        let mut resources = ResourceRequirements {
            memory_mb: None,
            cpu_cores: None,
            disk_mb: None,
            timeout_minutes,
        };

        // Parse quantitative resources
        for quant_resource in &job_xml.quantitative_resources {
            match quant_resource.name.to_lowercase().as_str() {
                "memory" | "mem" => {
                    resources.memory_mb = quant_resource.quantity.map(|q| q as u32);
                }
                "cpu" | "processors" => {
                    resources.cpu_cores = quant_resource.quantity.map(|q| q as u32);
                }
                "disk" | "storage" => {
                    resources.disk_mb = quant_resource.quantity.map(|q| q as u32);
                }
                _ => {}
            }
        }

        Ok(resources)
    }
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            default_timezone: "UTC".to_string(),
            strict_validation: false,
            default_timeout_minutes: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_xml() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <CONTROLM>
            <DEFTABLE>
                <FOLDER>
                    <FOLDER_NAME>TestFolder</FOLDER_NAME>
                    <JOB>
                        <JOBNAME>TestJob</JOBNAME>
                        <DESCRIPTION>Test job description</DESCRIPTION>
                        <CMDLINE>echo "Hello World"</CMDLINE>
                        <TASKTYPE>Command</TASKTYPE>
                    </JOB>
                </FOLDER>
            </DEFTABLE>
        </CONTROLM>
        "#;

        let parser = ControlMParser::new();
        let result = parser.parse_xml_str(xml_content);
        
        assert!(result.is_ok());
        let jobs = result.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "TestJob");
        assert_eq!(jobs[0].command, "echo \"Hello World\"");
    }
}
