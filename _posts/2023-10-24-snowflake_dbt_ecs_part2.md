---
title: Part 2 - Orchestrating Snowflake Data Transformations with DBT on Amazon ECS through Apache Airflow
author: jay
date: 2023-10-24 12:00:00 +/-0800
categories: [PROJECT]
tags: [snowflake,dbt,ecs ,mwaa, project]     # TAG names should always be lowercase
image:
  path: /assets/snowflake-dbt-ecs/arch_awsdbt.jpeg
  alt: Snowflake Data Transformations with DBT on Amazon ECS through Apache Airflow-part2
comments: true
---

## Overview:
In our previous post, we explored the setup of DBT on an ECR private repository through an AWS pipeline. In this blog, our emphasis will be on configuring MWAA and initiating DBT processes using Amazon's managed Apache Airflow (MWAA). Please find the source code on my [GitRepo](https://github.com/JayDataConsultant/snowflake-dbt-ecs).

## Architecture:

![img-description](/assets/snowflake-dbt-ecs/arch_awsdbt.jpeg)
_Architecture_

Refer to my previous blog for instructions on configuring DBT on an ECR Private Repository.
https://docs.google.com/document/d/1XXIOi5V63OjOXqJdyT_lPXarilhiBjs8nzHHx9dzc6o/edit

## MWAA:
[Amazon Managed Workflows](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html) allow developers the ability to quickly deploy an Airflow instance on AWS that utilises a combination of other AWS services to optimise the overall set-up.

![img-description](/assets/snowflake-dbt-ecs/MWAA.png)
_MWAA_

## STEP 1: To execute DBT within Airflow, the initial step is to establish MWAA.
Here are the steps for configuring MWAA:

  - 1.Select the S3 bucket from which your MWAA will retrieve the Directed Acyclic Graph (DAG) files.

![img-description](/assets/snowflake-dbt-ecs/MWAA_S3.png)
_MWAA S3 Bucket Config_

 - 2.Choose the Environment Class according to the number of DAGs run in your environment.

![img-description](/assets/snowflake-dbt-ecs/MWAA_ENV.png)
_MWAA ENV Config_

- 3.IAM Role permission for Airflow:
The following is a list of IAM permissions necessary to run our Airflow. Given that our Directed Acyclic Graph (DAG) is located in the S3 bucket, the MWAA role inherently has S3 bucket access. 

**IAM Role for SSM:**

Additionally, in our DAG, we'll be retrieving ECS cluster and Task details from the Parameter Store, which necessitates the required access.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "ssm:GetParameter",
            "Resource": "*"
        }
    ]
}
```

IAM Role for ECS Task Execution: Considering that we are making calls to the ECR repository, please ensure that the Task Execution policy has the necessary permissions.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecr:GetAuthorizationToken",
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

MWAA set up has been completed.

## STEP 2: To invoke DBT from Airflow, it is essential to configure the ECS Task definition and cluster.

Set up ECS Cluster and Task definition:


![img-description](/assets/snowflake-dbt-ecs/AWS_ECS.png)
_AWS ECS_

![img-description](/assets/snowflake-dbt-ecs/AWS_ECS_Running.png)
_AWS ECS running_

## STEP 3: Place your DAG Code in the S3 Bucket.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

from airflow.providers.amazon.aws.operators.ecs import  EcsRunTaskOperator


import boto3


ssm = boto3.client('ssm')

default_args={
        "start_date": days_ago(2),
        "owner": "jayaananth",
        "email": ["jayaananth@gmail.com"],
        "retries": 1,
        "retry_delay" :timedelta(minutes=5)
    }

with DAG("dbt_scd2_snowflake_ecs_operator", start_date=datetime(2022, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    


# Get ECS configuration from SSM parameters
    ecs_cluster               = str(ssm.get_parameter(Name='/mwaa/ecs/cluster', WithDecryption=True)['Parameter']['Value'])
    ecs_task_definition       = str(ssm.get_parameter(Name='/mwaa/ecs/task_definition', WithDecryption=True)['Parameter']['Value'])
    ecs_subnets               = str(ssm.get_parameter(Name='/mwaa/vpc/private_subnets', WithDecryption=True)['Parameter']['Value'])
    ecs_security_group        = str(ssm.get_parameter(Name='/mwaa/vpc/security_group', WithDecryption=True)['Parameter']['Value'])
##ecs_awslogs_group         = str(ssm.get_parameter(Name='/mwaa/cw/log_group', WithDecryption=True)['Parameter']['Value'])
#ecs_awslogs_stream_prefix = str(ssm.get_parameter(Name='/mwaa/cw/log_stream', WithDecryption=True)['Parameter']['Value'])
    print(ecs_task_definition)

# Run Docker container via ECS operator
    task_model_ecs_operator = EcsRunTaskOperator(
        task_id="snowflake_dbt_model_ecs_operator",
        dag=dag,
        aws_conn_id="aws_default",
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="FARGATE",
        overrides={
          "containerOverrides": [
                {
                  "name": "jay-snowflake",
                  "command": ["dbt","run","--select", "models/emp_fact.sql"]
                },
            ],
        },
        network_configuration={
           "awsvpcConfiguration": {
                "securityGroups": [ecs_security_group],
                "subnets": ecs_subnets.split(",") ,
            },
        },#
        # awslogs_group="ecs_awslogs_group",
        #awslogs_stream_prefix="ecs_awslogs_stream_prefix"
    )
    

    task_snapshot_ecs_operator = EcsRunTaskOperator(
        task_id="ecs_snowflake_operator",
        dag=dag,
        aws_conn_id="aws_default",
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="FARGATE",
        overrides={
          "containerOverrides": [
                {
                  "name": "jay-snowflake",
                  "command": ["dbt","snapshot","--select", "snapshots/scd_emp.sql"]
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": [ecs_security_group],
                "subnets": ecs_subnets.split(",") ,
            },
        },
        # awslogs_group="ecs_awslogs_group",
        #awslogs_stream_prefix="ecs_awslogs_stream_prefix"
    )
task_model_ecs_operator.set_downstream(task_snapshot_ecs_operator)

```

In the container override section, we will provide the ECR image name and specify the command to execute when Airflow triggers the job.

The DAG will retrieve the Task definition and cluster information from the Systems Manager (SSM) Parameter Store.

![img-description](/assets/snowflake-dbt-ecs/AWS_SSM.png)
_AWS SSM_
## STEP 4: Trigger your DAG.


![img-description](/assets/snowflake-dbt-ecs/MWAA_job.png)
_MWAA DAG Job_

In the image below, you can see your DAG executing the ECS Task function.


![img-description](/assets/snowflake-dbt-ecs/ECS_DAG.png)
_AWS ECS Task Function_

**Example:**
In the below example, we will capture the result pre and post after airflow jobs.

**Source:**

![img-description](/assets/snowflake-dbt-ecs/SRC_EMP.png)
_Source Table Employee_

![img-description](/assets/snowflake-dbt-ecs/SRC_DEPT.png)
_Source Table Department_

![img-description](/assets/snowflake-dbt-ecs/SRC_EMP_DEPT.png)
_Source Table Employee_Department_

**Target:**

![img-description](/assets/snowflake-dbt-ecs/TGT_Table.png)
_Target Emp_fact Table_

![img-description](/assets/snowflake-dbt-ecs/TGT_Table_2.png)
_Target Emp_fact Table_

Update the source record to capture the result.

```sql
UPDATE employee SET LAST_NAME='JAYARAM',
 updated_at=CURRENT_TIMESTAMP() WHERE emp_no=1; 
```

**Target post run:**

![img-description](/assets/snowflake-dbt-ecs/POST_TGT.png)
_Post Target Emp_fact Table_

![img-description](/assets/snowflake-dbt-ecs/POST_SCD_1.png)
_Post Target SCD_fact Table_

![img-description](/assets/snowflake-dbt-ecs/POST_SCD_2.png)
_Post Target SCD_fact Table_

## Advantages of Scheduled DBT Model Deployment for Snowflake Utilising AWS ECS and Airflow:
- **Automation:** Scheduled deployments in AWS ECS and Airflow automate the process of running DBT models, reducing manual intervention and minimising the risk of errors.

- **Efficiency:** Automation saves time and resources, making it more efficient to manage and update your DBT models, which can be particularly useful when dealing with large datasets.

- **Monitoring:** Airflow provides monitoring and logging capabilities, allowing you to track the progress and performance of your DBT tasks, making it easier to troubleshoot issues.

- **Scheduling Flexibility:** You can schedule DBT runs during off-peak hours or based on business requirements, ensuring that the data transformation processes do not impact regular operations.

- **Error Handling:** Airflow enables you to set up error-handling mechanisms, such as notifications or retries, to ensure that your DBT tasks are robust and resilient.



## Conclusion:
In this blog, we have explored the construction of Snowflake Data Transformations using DBT on Amazon ECS within the context of Apache Airflow. This approach offers a versatile and all-encompassing structure for organisations to improve their data transformation processes.




**Note:** This article was originally published on [ Cevo Australia’s website ](https://cevo.com.au/post/orchestrating-snowflake-data-transformations-with-dbt-on-amazon-ecs-through-apache-airflow-part-2//)


## If you enjoy the article, consider supporting me by buying a coffee for $1.


<script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="jayaananth" data-color="#FFDD00" data-emoji="☕"  data-font="Cookie" data-text="Buy me a coffee @ 1$" data-outline-color="#000000" data-font-color="#000000" data-coffee-color="#ffffff" ></script>


<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-4606733459883553"
     crossorigin="anonymous"></script>