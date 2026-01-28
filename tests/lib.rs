//! Integration tests for the Airflow Enterprise Toolkit
//! 
//! This module contains integration tests that verify the end-to-end functionality
//! of the Control-M to Airflow conversion process.

use airflow_enterprise_toolkit::application::services::{DagGenerator, JobValidator};
use airflow_enterprise_toolkit::domain::entities::*;
use airflow_enterprise_toolkit::infrastructure::parsers::ControlMParser;
use chrono::Utc;
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_control_m_xml() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <CONTROLM>
            <FOLDER>
                <FOLDER_NAME>TestFolder</FOLDER_NAME>
                <DESCRIPTION>Test folder</DESCRIPTION>
                <JOB>
                    <JOBNAME>TestJob</JOBNAME>
                    <DESCRIPTION>Test job description</DESCRIPTION>
                    <COMMAND>echo "Hello World"</COMMAND>
                    <JOB_TYPE>Command</JOB_TYPE>
                    <TIMEOUT>30</TIMEOUT>
                </JOB>
            </FOLDER>
        </CONTROLM>
        "#;

        let parser = ControlMParser::new();
        let result = parser.parse_xml_str(xml_content);
        
        assert!(result.is_ok());
        let jobs = result.unwrap();
        assert_eq!(jobs.len(), 1);
        
        let job = &jobs[0];
        assert_eq!(job.name, "TestJob");
        assert_eq!(job.command, "echo \"Hello World\"");
        assert_eq!(job.job_type, JobType::Command);
        assert_eq!(job.resources.timeout_minutes, Some(30));
    }

    #[test]
    fn test_parse_job_with_dependencies() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <CONTROLM>
            <FOLDER>
                <FOLDER_NAME>TestFolder</FOLDER_NAME>
                <JOB>
                    <JOBNAME>JobA</JOBNAME>
                    <COMMAND>echo "Job A"</COMMAND>
                    <JOB_TYPE>Command</JOB_TYPE>
                </JOB>
                <JOB>
                    <JOBNAME>JobB</JOBNAME>
                    <COMMAND>echo "Job B"</COMMAND>
                    <JOB_TYPE>Command</JOB_TYPE>
                    <INCOND>
                        <NAME>JobA</NAME>
                        <OPERATOR>EQ</OPERATOR>
                    </INCOND>
                </JOB>
            </FOLDER>
        </CONTROLM>
        "#;

        let parser = ControlMParser::new();
        let result = parser.parse_xml_str(xml_content);
        
        assert!(result.is_ok());
        let jobs = result.unwrap();
        assert_eq!(jobs.len(), 2);
        
        let job_b = jobs.iter().find(|j| j.name == "JobB").unwrap();
        assert_eq!(job_b.dependencies.len(), 1);
        assert_eq!(job_b.dependencies[0].job_name, "JobA");
        assert_eq!(job_b.dependencies[0].dependency_type, DependencyType::Success);
    }

    #[test]
    fn test_parse_job_with_hold_conditions() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <CONTROLM>
            <FOLDER>
                <FOLDER_NAME>TestFolder</FOLDER_NAME>
                <JOB>
                    <JOBNAME>WaitForFile</JOBNAME>
                    <COMMAND>echo "File received"</COMMAND>
                    <JOB_TYPE>Command</JOB_TYPE>
                    <INCOND>
                        <NAME>FILE_HOLD:/data/input.txt</NAME>
                        <OPERATOR>EQ</OPERATOR>
                    </INCOND>
                </JOB>
            </FOLDER>
        </CONTROLM>
        "#;

        let parser = ControlMParser::new();
        let result = parser.parse_xml_str(xml_content);
        
        assert!(result.is_ok());
        let jobs = result.unwrap();
        assert_eq!(jobs.len(), 1);
        
        let job = &jobs[0];
        assert_eq!(job.hold_conditions.len(), 1);
        assert!(matches!(job.hold_conditions[0].condition_type, HoldType::FileHold));
    }

    #[test]
    fn test_parse_order_date_handling() {
        let xml_content = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <CONTROLM>
            <FOLDER>
                <FOLDER_NAME>TestFolder</FOLDER_NAME>
                <JOB>
                    <JOBNAME>OrderDateJob</JOBNAME>
                    <COMMAND>echo "Order date: {{ORDER_DATE}}"</COMMAND>
                    <JOB_TYPE>Command</JOB_TYPE>
                    <SCHEDULE>
                        <ORDER_DATE>PreviousBusinessDay</ORDER_DATE>
                    </SCHEDULE>
                </JOB>
            </FOLDER>
        </CONTROLM>
        "#;

        let parser = ControlMParser::new();
        let result = parser.parse_xml_str(xml_content);
        
        assert!(result.is_ok());
        let jobs = result.unwrap();
        assert_eq!(jobs.len(), 1);
        
        let job = &jobs[0];
        assert_eq!(job.schedule.order_date, OrderDateHandling::PreviousBusinessDay);
    }

    #[test]
    fn test_convert_single_job_to_dag() {
        let job = ControlMJob::new(
            "TestJob".to_string(),
            "echo 'Hello World'".to_string(),
            JobType::Command,
        );

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        assert_eq!(dag.dag_id, "control_m_testjob");
        assert_eq!(dag.tasks.len(), 1);
        
        let task = &dag.tasks[0];
        assert_eq!(task.task_id, "testjob");
        assert_eq!(task.operator_type, "BashOperator");
    }

    #[test]
    fn test_convert_job_with_order_date() {
        let mut job = ControlMJob::new(
            "OrderDateJob".to_string(),
            "echo 'Order date: {{ORDER_DATE}}'".to_string(),
            JobType::Command,
        );
        job.schedule.order_date = OrderDateHandling::PreviousBusinessDay;

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        let task = &dag.tasks[0];
        
        assert!(task.order_date_config.is_some());
        let order_date_config = task.order_date_config.as_ref().unwrap();
        assert_eq!(order_date_config.strategy, OrderDateHandling::PreviousBusinessDay);
        assert!(order_date_config.template.contains("ds_add"));
    }

    #[test]
    fn test_validate_single_job() {
        let job = ControlMJob::new(
            "ValidJob".to_string(),
            "echo 'Valid command'".to_string(),
            JobType::Command,
        );

        let result = JobValidator::validate_job(&job);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_invalid_job() {
        let job = ControlMJob::new(
            "".to_string(), // Empty name should fail validation
            "echo 'Valid command'".to_string(),
            JobType::Command,
        );

        let result = JobValidator::validate_job(&job);
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_circular_dependencies() {
        let mut job_a = ControlMJob::new(
            "JobA".to_string(),
            "echo 'Job A'".to_string(),
            JobType::Command,
        );
        job_a.add_dependency("JobC".to_string(), DependencyType::Success);

        let mut job_b = ControlMJob::new(
            "JobB".to_string(),
            "echo 'Job B'".to_string(),
            JobType::Command,
        );
        job_b.add_dependency("JobA".to_string(), DependencyType::Success);

        let mut job_c = ControlMJob::new(
            "JobC".to_string(),
            "echo 'Job C'".to_string(),
            JobType::Command,
        );
        job_c.add_dependency("JobB".to_string(), DependencyType::Success);

        let jobs = vec![job_a, job_b, job_c];
        let result = JobValidator::validate_jobs(&jobs);
        
        // Should detect circular dependency
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_script_job() {
        let job = ControlMJob::new(
            "PythonJob".to_string(),
            "print('Hello Python')".to_string(),
            JobType::Script {
                interpreter: "python".to_string(),
            },
        );

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        let task = &dag.tasks[0];
        assert_eq!(task.operator_type, "PythonOperator");
    }

    #[test]
    fn test_convert_database_job() {
        let job = ControlMJob::new(
            "DatabaseJob".to_string(),
            "SELECT * FROM table".to_string(),
            JobType::Database {
                db_type: "postgres".to_string(),
            },
        );

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        let task = &dag.tasks[0];
        assert_eq!(task.operator_type, "SqlOperator");
    }

    #[test]
    fn test_job_with_environment_variables() {
        let mut job = ControlMJob::new(
            "EnvJob".to_string(),
            "echo $MY_VAR".to_string(),
            JobType::Command,
        );
        job.add_environment_variable("MY_VAR".to_string(), "test_value".to_string());

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        let task = &dag.tasks[0];
        assert!(task.config.contains_key("env"));
    }

    #[test]
    fn test_job_with_resource_requirements() {
        let mut job = ControlMJob::new(
            "ResourceJob".to_string(),
            "echo 'Resource test'".to_string(),
            JobType::Command,
        );
        job.resources.timeout_minutes = Some(120);

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        let task = &dag.tasks[0];
        assert!(task.config.contains_key("execution_timeout"));
    }

    #[test]
    fn test_multiple_jobs_in_single_dag() {
        let job1 = ControlMJob::new(
            "Job1".to_string(),
            "echo 'Job 1'".to_string(),
            JobType::Command,
        );
        let job2 = ControlMJob::new(
            "Job2".to_string(),
            "echo 'Job 2'".to_string(),
            JobType::Command,
        );

        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![job1, job2]);
        
        assert!(result.is_ok());
        let dag = result.unwrap();
        assert_eq!(dag.tasks.len(), 2);
        assert!(dag.dag_id.starts_with("control_m_group_"));
    }

    #[test]
    fn test_empty_job_list() {
        let generator = DagGenerator::new();
        let result = generator.generate_dag(vec![]);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No jobs provided"));
    }
}
