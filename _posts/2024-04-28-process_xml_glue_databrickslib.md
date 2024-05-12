---
title: Streamline Onix 3.0 Multi-Nested XML Processing - AWS Glue with DynamicFrames, Relationalize, and Databricks Spark-XML
author: jay
date: 2024-04-28 12:00:00 +/-0800
categories: [AWS Glue]
tags: [XML,aws glue,S3 ,spark, project]     # TAG names should always be lowercase
image:
  path: /assets/xml_glue_databrickslib/AWS Glue- Using_DynamicFrame,Relationalize,Databricks Spark-XML.png
  alt: How to  Process Multi-Nested XML Files(Onix3.0) with AWS Glue- Using DynamicFrame,Relationalize,Databricks Spark-XML
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

## Introduction:
Nowadays, XML files are the go-to option for many industries, including finance, books, and healthcare, for storing and exchanging data. Though XML files are widely used, analyzing and processing them can be difficult, particularly when dealing with highly nested schemas, various nested data structures, and dynamic changes in data types (also known as schema evolution).

In this blog post, we'll explore how we can address these challenges using AWS Glue, DynamicFrames, Relationalize, and the Databricks Spark XML. 

## Solution Overview:

## Method 1:  By using Aws Glue and Dynamic Frame 

To handle XML files, using AWS Glue and DynamicFrame is a common method. We may investigate the data types received from the source, whether they are ArrayType, StructType, or a combination of the two, by reading XML files with DynamicFrame. This approach, however, necessitates closely examining every tag and processing records appropriately. The code gets more complex as the files get bigger and the schema gets more complicated, especially with heavily nested schemas.

**Prerequisites**

1) Get the sample files from the link.  [GitRepo](https://github.com/JayaananthGitRepo/Glue/blob/main/Glue_XML/sample_files/book_retail_df.xml). Upload the sample files in the S3 Bucket.

![img-description](/assets/xml_glue_databrickslib/S3FileUpload.png)
_File Upload in S3_

2) Launch Glue Studio and choose the Notebook option.

![img-description](/assets/xml_glue_databrickslib/GlueStudio-Notebook.png)
_Glue Notebook_

3) Here's a Python code snippet using AWS Glue to read an XML file using DynamicFrame and print its schema structure:

```python

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col, slice, size, when, array_contains, to_date, lit, broadcast, split, udf,explode
from pyspark.sql.types import BooleanType


args = {'JOB_NAME':'book-test', 'database_name':'iceberg_dataplatform', 'input_path':'s3://glue-xml-file/bookdetail/3', 'output_path':'s3://glue-xml-file/warehouse/output'}

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
input_path = args['input_path']
output_path = args['output_path']
database_name = args['database_name']
catalog_name = "glue_catalog"

warehouse_path = f"s3://glue-xml-file/warehouse/"


glue_temp_storage = f"s3://glue-xml-file/warehouse/GLUE_TMP"
s3path = {"paths": [input_path]}
print(f"Loading data from {s3path}")


#Read the XML File Using Dynamic Frame
product_options = {"rowTag": "Product", "attachFilename": "book_format"}
dynamicFrameProducts = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options=s3path,
    format="xml",
    format_options=product_options,
    transformation_ctx="dynamicFrameProducts",
)


#convert the dynamic frame to dataframe
product_df = dynamicFrameProducts.toDF()
#print the schema
product_df.printSchema()

```

**Schema Output:**

The below schema has both Array Type and Struct Type

![img-description](/assets/xml_glue_databrickslib/Schema_structure_method_1.png)
_Schema Structure_

After you've determined the schema structure, extract the values and display them in a tabular style using the following Python snippet. This simplified method will provide a smooth integration into your intended tables.

``` python
# Prepare the Person Name
# Get the Data Type and check if Array or struct Data type 
name_type = product_df.schema["DescriptiveDetail"].dataType["Contributor"].dataType

if "array" in name_type.typeName():
    print('array')
    product_df = product_df.withColumn("name", explode("DescriptiveDetail.Contributor"))
    product_df = product_df.withColumn("PersonName", col("name.PersonName"))

elif "struct" in name_type.typeName():
    print('struct')
    product_df = product_df.withColumn("PersonName", when(col("DescriptiveDetail.Contributor.struct.PersonName").isNotNull(),
                                                          col("DescriptiveDetail.Contributor.struct.PersonName")
     ).otherwise(None))

# Prepare the Language
language_type = product_df.schema["DescriptiveDetail"].dataType["Language"].dataType

if "array" in language_type.typeName():
    print('lang array')
    product_df = product_df.withColumn("Language", explode("DescriptiveDetail.Language"))
    product_df = product_df.withColumn("Language", when (col("Language.LanguageRole") == 1, col("Language.LanguageCode")).otherwise(None))

elif "struct" in language_type.typeName():
    print(' lang struct')
    product_df = product_df.withColumn("Language", when(col("DescriptiveDetail.Language.LanguageRole") ==1,
                                                          col("DescriptiveDetail.Language.LanguageCode")
     ).otherwise(None))

    # Prepare the book_id
product_type = product_df.schema["ProductIdentifier"].dataType

if "array" in product_type.typeName():
    print('product array')
    product_df = product_df.withColumn("book", explode("ProductIdentifier"))
    product_df = product_df.withColumn("book_id",col("book.book_id")) 

elif "struct" in product_type.typeName():
    print(' product struct')
    product_df = product_df.withColumn("book_id", col("ProductIdentifier.IDValue"))
    

book_df=product_df.select(col("book_id"),col("PersonName"),col("Language")).show()

```

**Result:**

![img-description](/assets/xml_glue_databrickslib/Dynamic_Result.png)
_Final Output_

This method works well even when the schema structure isn't stated explicitly. However, the coding complexity dramatically rises with the amount of tags and hierarchical schema structures.
 
## Method 2: By using Aws Glue ,Databricks library and Relationalize

[Relationalize](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-Relationalize.html) will flatten the nested structure. While reading the XML files using the DataBricks Spark-Xml, it will provide more precise control over parsing complex XML structures. [SampleFile](https://github.com/JayaananthGitRepo/Glue/blob/main/Glue_XML/sample_files/book_reletionalize.xml)

**Prerequisite**

1) **Download the Databricks Spark-XML JAR File:** Obtain the Databricks Spark-XML JAR file from the.[MVNRepository](https://mvnrepository.com/artifact/com.databricks/spark-xml_2.12/0.13.0)

2) **Upload the JAR File to an S3 Bucket:** Upload the downloaded JAR file to an S3 bucket that your AWS Glue job can access.

![img-description](/assets/xml_glue_databrickslib/S3_Databricks_jarfile.png)
_DataBrciks Jar File_

3) **Configure Your AWS Glue Notebook:** In the first cell of your AWS Glue notebook, add the following code to ensure that the Databricks Spark-XML library is available to your job

```python
%idle_timeout 30
%glue_version 4.0
%%configure
{
        "--extra-jars": "s3://glue-xml-file/jarfile/spark-xml_2.12-0.13.0.jar",
        "--conf": "spark.jars.packages=com.databricks:spark-xml_2.12:0.13.0"
    
}

```

You can observe how DataBricks Spark-XML reads XML more effectively than dynamicframe work in the example below.

**Schema for Dynamic Frame Work**

![img-description](/assets/xml_glue_databrickslib/DynamicFrame_struct_xml.png)
_Dynamic Frame Schema Structure_

**Schema for Databricks-Spark Xml**

![img-description](/assets/xml_glue_databrickslib/Databricks_struct_xml.png)
_Databrick Schema Structure_

The below snippet is to read the XML file using DataBrick spark-XML

```python

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col, slice, size, when, array_contains, to_date, lit, broadcast, split, udf,explode
from pyspark.sql.types import BooleanType


args = {'JOB_NAME':'book-test', 'database_name':'iceberg_dataplatform', 'input_path':'s3://glue-xml-file/bookdetail/2', 'output_path':'s3://glue-xml-file/warehouse/output'}

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
input_path = args['input_path']
output_path = args['output_path']
database_name = args['database_name']
catalog_name = "glue_catalog"

warehouse_path = f"s3://glue-xml-file/warehouse/"
xml_input_path = f"{input_path}/"

glue_temp_storage = f"s3://glue-xml-file/warehouse/GLUE_TMP"
s3path = {"paths": [input_path]}
print(f"Loading data from {s3path}")

product_df = spark.read.format('xml').options(rowTag='Product', excludeAttribute=True).load(f'{xml_input_path}')

#product_df = dynamicFrameProducts.toDF()
product_df.printSchema()

```

The relationalize function will flatten the nested structure, storing it as keys in a tabular format. Utilize the following snippet to achieve the flattening.The relationalize needs the S3 path to store the flattened structure

``` python
dyf = DynamicFrame.fromDF(product_df, glueContext, "dyf")
dfc = dyf.relationalize("root", "s3://glue-xml-file/temp-dir/")
dfc.keys()
all_keys = list(dfc.keys())
print(all_keys)

```

**Output**

![img-description](/assets/xml_glue_databrickslib/relationalize_keys.png)
_Relationalize Key_

**Use the below snippet to read the root and the sub keys**

```python

dyf = DynamicFrame.fromDF(product_df, glueContext, "dyf")
dfc = dyf.relationalize("root", "s3://glue-xml-file/temp-dir/")
dfc.keys()
all_keys = list(dfc.keys())
print(all_keys)
final_df = dfc.select('root')
final_df=final_df.toDF()
final_df.show()

```
**Output**

![img-description](/assets/xml_glue_databrickslib/relationalize_root_key.png)
_Root Result_


```python
dyf = DynamicFrame.fromDF(product_df, glueContext, "dyf")
dfc = dyf.relationalize("root", "s3://glue-xml-file/temp-dir/")
dfc.keys()
all_keys = list(dfc.keys())
print(all_keys)
fin_df = dfc.select('root_DescriptiveDetail.Language')
fin_df=fin_df.toDF()
fin_df.show()

```

**Output**

![img-description](/assets/xml_glue_databrickslib/relationalize_sub_key.png)
_Sub Key Result_


The next step is to join the root key and the other sub keys. The root key will have the reference identifier in the sub keys table

For example, Root key Identifier “DescriptiveDetail.Language” and the subkey identifier “id". In the below code, we will be joining the keys to extract of the flattening data


```python

dyf = DynamicFrame.fromDF(product_df, glueContext, "dyf")
dfc = dyf.relationalize("root", "s3://glue-xml-file/temp-dir/")
dfc.keys()
all_keys = list(dfc.keys())
print(all_keys)
final_df = dfc.select('root')
final_df=final_df.toDF()

fin_df = dfc.select('root_DescriptiveDetail.Language')
fin_df=fin_df.toDF()
fin_df.createOrReplaceTempView(f"language")
final_df.createOrReplaceTempView(f"final_df")

result=spark.sql(""" select * from final_df fd join
           language ct on fd.`DescriptiveDetail.Language` =ct.id""")

result.createOrReplaceTempView(f"language")


spark.sql("""select `ProductIdentifier.IDValue` isbn,`DescriptiveDetail.Language.val.LanguageCode` LanguageCode,
`DescriptiveDetail.Language.val.LanguageRole` LanguageRole from language
 """).show()

 ```
**Final Result**

 ![img-description](/assets/xml_glue_databrickslib/Reletionalize_result.png)
_FinalResult_


To include the Databricks Spark-XML JAR file in the AWS Glue job, you need to specify the S3 bucket path and provide configuration details in the job parameters as illustrated below.

**"--conf": "spark.jars.packages=com.databricks:spark-xml_2.12:0.13.0"**



![img-description](/assets/xml_glue_databrickslib/Glue-job-Library.png)
_Glue Library_

## Method 3: Using AWS Glue,Dynamic Frame and Declaring the Schema.

In this approach, we'll employ the dynamic frame framework to read the XML, wherein we'll assign the nested schema to the defined schema structure. This allows us to understand the structure beforehand, facilitating the straightforward reading of multi-nested files.


```python

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col, slice, size, when, array_contains, to_date, lit, broadcast, split, udf,explode
from pyspark.sql.types import BooleanType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType
)
from pyspark.sql.functions import col, udf, from_json


args = {'JOB_NAME':'book-test', 'database_name':'iceberg_dataplatform', 'input_path':'s3://glue-xml-file/bookdetail/2', 'output_path':'s3://glue-xml-file/warehouse/output'}

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
input_path = args['input_path']
output_path = args['output_path']
database_name = args['database_name']
catalog_name = "glue_catalog"

warehouse_path = f"s3://glue-xml-file/warehouse/"


glue_temp_storage = f"s3://glue-xml-file/warehouse/GLUE_TMP"
s3path = {"paths": [input_path]}
print(f"Loading data from {s3path}")

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()



product_options = {"rowTag": "Product", "attachFilename": "book_format"}
dynamicFrameProducts = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options=s3path,
    format="xml",
    format_options=product_options,
    transformation_ctx="dynamicFrameProducts",
)


product_df = dynamicFrameProducts.toDF()
product_df.printSchema()



schema=ArrayType(StructType([StructField('LanguageCode', StringType()),StructField('LanguageRole', StringType())]))

datachoice0 = ResolveChoice.apply(
    frame=dynamicFrameProducts,
    choice="cast:string",
    transformation_ctx="datachoice0",
)


# In string form, look to see if the string is in a square bracket [, indicating an array, if not add them
@udf(returnType=StringType())
def struct_to_array(order):
    if order:
        return f"[{order}]" if order[:1] != "[" else order
    # Handle case where "array" is empty
    return "[]"

map0 = datachoice0.toDF().withColumn(
    "Language_array", from_json(struct_to_array(col("DescriptiveDetail.Language")),schema)
)
fromdataframe = DynamicFrame.fromDF(map0, glueContext, "fromdataframe0")
fromdataframe=fromdataframe.toDF()
fromdataframe.printSchema()


schema=ArrayType(StructType([StructField('LanguageCode', StringType()),StructField('LanguageRole', StringType())]))

datachoice0 = ResolveChoice.apply(
    frame=dynamicFrameProducts,
    choice="cast:string",
    transformation_ctx="datachoice0",
)


# In string form, look to see if the string is in a square bracket [, indicating an array, if not add them
@udf(returnType=StringType())
def struct_to_array(order):
    if order:
        return f"[{order}]" if order[:1] != "[" else order
    # Handle case where "array" is empty
    return "[]"

map0 = datachoice0.toDF().withColumn(
    "Language_array", from_json(struct_to_array(col("DescriptiveDetail.Language")),schema)
)
fromdataframe = DynamicFrame.fromDF(map0, glueContext, "fromdataframe0")
product_df=fromdataframe.toDF()
product_df.printSchema()
product_df.createOrReplaceTempView(f"language")
#spark.sql(""" select * from Language""").show()


language_type = product_df.schema["Language_array"].dataType
if "array" in language_type.typeName():
    print('lang array')
    product_df = product_df.withColumn("Language", explode("Language_array"))
    product_df = product_df.withColumn("Language", when (col("Language.LanguageRole") == 1, col("Language.LanguageCode")).otherwise(None))

elif "struct" in language_type.typeName():
    print(' lang struct')
    product_df = product_df.withColumn("Language", when(col("Language_array.LanguageRole") ==1,
                                                          col("Language_array.LanguageCode")
     ).otherwise(None))

    # Prepare the book_id
product_type = product_df.schema["ProductIdentifier"].dataType

if "array" in product_type.typeName():
    print('ID array')
    product_df = product_df.withColumn("book", explode("ProductIdentifier"))
    product_df = product_df.withColumn("book_id",col("book.book_id")) 

elif "struct" in product_type.typeName():
    print(' ID struct')
    product_df = product_df.withColumn("book_id", col("ProductIdentifier.IDValue"))
    


#product_df.printSchema()
book_df=product_df.select(col("book_id"),col("Language")).show()

```

**Output**

![img-description](/assets/xml_glue_databrickslib/Schema_result.png)
_Schema Result_


## Summary:

This blog extensively explores the efficient methods for reading multi-nested schema structures, including dynamic data type changes, through the utilization of DynamicFrame, Relationalize, Databricks Spark-XML, and schema definition.

 Don't forget to subscribe to the page for updates, and visit the associated GitHub repository where the complete code is available. It showcases how we can dynamically process multiple files simultaneously, each with different data types, using Relationalize.


**If you enjoy the article, Please Subscribe.**
<script type="text/javascript" src="https://cdn.jsdelivr.net/npm/@mc-dpo/analytics@4.1.0/dist/bundle.min.js"></script>
<script id="mcjs">!function(c,h,i,m,p){m=c.createElement(h),p=c.getElementsByTagName(h)[0],m.async=1,m.src=i,p.parentNode.insertBefore(m,p)}(document,"script","https://chimpstatic.com/mcjs-connected/js/users/65109540a509d2f586be01728/1d000ea39b3fbba96cb5f9a1f.js");</script>
<div id="mc_embed_signup">
    <form action="https://datainevitable.us9.list-manage.com/subscribe/post?u=1d9ad4c0fcc2fda62ecb888e4&amp;id=0c400e90a2&amp;f_id=00481ee1f0" method="post" id="mc-embedded-subscribe-form" name="mc-embedded-subscribe-form" class="validate" target="_blank">
        <div id="mc_embed_signup_scroll">
            <h2>This blog page is designed to keep you informed anything and everything about data and to support your career growth</h2>
            <div class="indicates-required"><span class="asterisk">*</span> indicates required</div>
            <div class="mc-field-group">
                <label for="mce-EMAIL">Email Address <span class="asterisk">*</span></label>
                <input type="email" name="EMAIL" class="required email" id="mce-EMAIL" required="" value="">
            </div>
            <div id="mce-responses" class="clear foot">
                <div class="response" id="mce-error-response" style="display: none;"></div>
                <div class="response" id="mce-success-response" style="display: none;"></div>
            </div>
            <div aria-hidden="true" style="position: absolute; left: -5000px;">
                <input type="text" name="b_1d9ad4c0fcc2fda62ecb888e4_0c400e90a2" tabindex="-1" value="">
            </div>
            <div class="optionalParent">
                <div class="clear foot">
                    <input type="submit" name="subscribe" id="mc-embedded-subscribe" class="button" value="Subscribe">
                    <p style="margin: 0px auto;"><a href="https://eepurl.com/iClX3I" title="Mailchimp - email marketing made easy and fun"><span style="display: inline-block; background-color: transparent; border-radius: 4px;"><img class="refferal_badge" src="https://digitalasset.intuit.com/render/content/dam/intuit/mc-fe/en_us/images/intuit-mc-rewards-text-dark.svg" alt="Intuit Mailchimp" style="width: 220px; height: 40px; display: flex; padding: 2px 0px; justify-content: center; align-items: center;"></span></a></p>
                </div>
            </div>
        </div>
    </form>
</div>


## If you love the article, Please consider supporting me by buying a coffee for $1.


<script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="jayaananth" data-color="#FFDD00" data-emoji="☕"  data-font="Cookie" data-text="Buy me a coffee @ 1$" data-outline-color="#000000" data-font-color="#000000" data-coffee-color="#ffffff" ></script>


<<meta name="google-adsense-account" content="ca-pub-4606733459883553">