---
title: Efficient XML Data Processing and Querying with AWS Glue and Athena - A Comprehensive Guide
author: jay
date: 2024-03-11 12:00:00 +/-0800
categories: [AWS Athena]
tags: [glue,athena,xml,glue catalog,glue crawler]     # TAG names should always be lowercase
image:
  path: /assets/glue_xml_athena/XML_AWS_ATHENA.png
  alt: XML File Processing with Athena 
comments: true
---

<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-56G57XP8PY"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-56G57XP8PY');
</script>

## Introduction
In today's data-driven landscape, the analysis of XML files holds significant importance across diverse industries such as finance, healthcare, and government. The utilization of XML data facilitates organizations in gaining valuable insights, enhancing decision-making processes, and streamlining data integration efforts. However, the semi-structured and highly nested nature of XML files presents challenges in accessing and analyzing information, particularly in the case of large and complex schemas.

In this blog, we will delve into the process of reading XML files in a tabular format using Amazon Athena, leveraging AWS Glue for cataloging, classification, and Parquet-based processing. This comprehensive guide will provide insights into effectively handling XML data within the AWS environment, enabling seamless querying and analysis using Athena.


## Prerequisites
Before you begin this tutorial, complete the following prerequisites:
1. Download the sample files from [ kaggle site ](https://www.kaggle.com/datasets/amritanshusharma23/demo-xml)
2. Establish an S3 Bucket and upload the sample files to it.
3. Set up all the essential IAM roles and policies.
4. Generate an AWS Database utilizing Glue.

**Steps to processs the XML file** 

(i) Before creating a crawler we need to create an xml classifier where we define the rowtag 

  ![img-description](/assets/glue_xml_athena/classifier_xml.png)
_AWS Classifier_

(ii) Generate an AWS Glue crawler to extract metadata from XML files, then execute the crawler to generate a table in the AWS Glue Data Catalog.

  ![img-description](/assets/glue_xml_athena/crawler_s3_classifier.png)
_AWS Glue Catalog_

  ![img-description](/assets/glue_xml_athena/crawler_s3_classifier.png)
_AWS Glue Database_

(iii) Go to Athena to view the table and query it

If we try to query the XML table the query will be failed since Athena wont support XML format

  ![img-description](/assets/glue_xml_athena/xml_query.png)
_Query XML Table Athena_

(iv) Generate a Glue job to convert XML files to the Parquet format, which can be accomplished using two different methods.
      Method 1- Use Visual editor from Glue anc convert from XML to Parquet format
      Method 2 - Use script editor and write the script. In this example I have used pyspark

**Method 1:** 

  **Step 1 :** Go to AWS Glue and select ETL Jobs -> Visual ETL

  **Step 2 :** Choose AWS Glue Data Catalog as Source and select your respective table as below
                  ![img-description](/assets/glue_xml_athena/glue_source.png)
                  _Glue Visual Editor Source_

  **Step 3 :** Under transformation choose Change Schema.

  **Step 4 :** Under target select S3 bucket ,parquet format and snappy compression as below 
        ![img-description](/assets/glue_xml_athena/glue_target.png)
        _Glue Visual Editor Target_

  **Step 5 :** Choose the respective IAM Role and Run the Glue Job

**Method 2:** 

   **Step 1 :** Go to AWS Glue and select ETL Jobs -> Visual ETL -> Script editor  
   
  **Step 2 :** Copy paste the below code and run the job

{% highlight python %}
{% raw %}
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Define input and output paths
input_path = "s3://glue-xml-file/"
output_path = "s3://glue-parquet-file/"

# Read XML data as DynamicFrame
xml_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database = "glue-etl", table_name = "xml_glue_xml_file")

# Convert specific data types to string using Spark SQL functions
xml_dynamic_frame_df = xml_dynamic_frame.toDF()


# Write data to Parquet format
glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(xml_dynamic_frame_df, glueContext, "transformed_df"), connection_type = "s3", connection_options = {"path": output_path}, format = "parquet",mode="overwrite")


# Commit the job
job.commit()
{% endraw %}
{% endhighlight %}


(v) Create an AWS Glue crawler to extract Parquet metadata and run the crawler which will create a table in the AWS Glue Data Catalog (Choose the same classifier that we created in the first steps)

![img-description](/assets/glue_xml_athena/parquet_crawler_src.png)
_AWS Crawler Parquet Source_

![img-description](/assets/glue_xml_athena/parquet_crawler_output.png)
_AWS Crawler Parquet Output_


(vi) Query the table in Athena and the result will be in the JSON format

![img-description](/assets/glue_xml_athena/parquet_athena.png)
_Query Parquet Table Using Athena_

(vii) To display the JSON data in a tabular format, we must unnest the column. It is essential to grasp the table's schema before proceeding with the unnesting process.

{% highlight python %}
{% raw %}
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet File from S3") \
    .getOrCreate()

# S3 path where the Parquet file is located
s3_path = "s3a://glue-parquet-file/"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_path)

# Print the schema of the DataFrame
df.printSchema()

# Stop the SparkSession
spark.stop()
{% endraw %}
{% endhighlight %}

Schema Structure of the Table

![img-description](/assets/glue_xml_athena/printSchema.png)
_Schema Structure Of Parquet Table_


![img-description](/assets/glue_xml_athena/parquet_unest.png)
_Unnesting the Table using SQL_


To query the table with nested structure

{% highlight sql %}
{% raw %}
SELECT 
    con.contributorrole,
    nid.idtypename,
    con
FROM 
    "glue-etl"."parquet_glue_parquet_file"
CROSS JOIN 
    UNNEST(contributor) AS t(con)
CROSS JOIN 
    UNNEST(con.NameIdentifier) AS t(nid);
{% endraw %}
{% endhighlight %}


![img-description](/assets/glue_xml_athena/parquest_nestesd_unnest.png)
_Unnesting the nested Table using SQL_

## Conclusion

In this article, we have explored the process of reading an XML file using Athena and flattening the table structure for easier analysis. This approach enables users to validate the source data prior to further processing. In our next publication, we will delve into the detailed steps involved in processing XML files using AWS Glue, enhancing the understanding and utilization of these powerful AWS services.




**If you enjoy the article, Please Subscribe.**
## If you love the article, Please consider supporting me by buying a coffee for $1.


<script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="jayaananth" data-color="#FFDD00" data-emoji="☕"  data-font="Cookie" data-text="Buy me a coffee @ 1$" data-outline-color="#000000" data-font-color="#000000" data-coffee-color="#ffffff" ></script>


<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-4606733459883553"
     crossorigin="anonymous"></script>