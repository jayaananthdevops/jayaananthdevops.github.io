---
title: "Dynamic Table in Snowflake : Implementing Type 2 Slowly Changing Dimensions (SCD) with Flexibility and Efficiency"
author: jay
date: 2023-09-27 12:00:00 +/-0800
categories: [Snowflake]
tags: [snowflake,snowflake dynamictable,scd2]     # TAG names should always be lowercase
image:
  path: SF_dynamics_tables.png
  alt: Snowflake Dynamic Table
toc: true
img_path: /assets/dynamictable/
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

Snowflake introduced ‘Dynamic Tables’ as a new feature in 2022. Dynamic tables can significantly simplify data engineering in Snowflake by providing a reliable, cost-effective, and automated way to transform data for consumption.

In this blog, we will discuss what a dynamic table is, when to use it and we’ll also build SCD type 2 using the dynamic table with an example.

## Dynamic Table:

[Dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about) are a new table type in Snowflake that lets teams use simple SQL statements to declaratively define the result of data pipelines. They also automatically refresh as the data changes, only operating on new changes since the last refresh. The scheduling and orchestration needed to achieve this are also transparently managed by Snowflake.

## Comparison of materialised views with dynamic tables
[Materialised view vs dynamic table](https://docs.snowflake.com/en/user-guide/dynamic-tables-comparison#dynamic-tables-compared-to-materialized-views):

Dynamic tables come with certain advantages, use cases and of course, challenges.

**Advantages:**
-  **Flexibility:** Dynamic tables offer flexibility in that they can alter their structure or schema on the fly. Depending on particular circumstances or occurrences, columns may be added, modified, or eliminated.
-  **Data Structures:** Dynamic tables can handle structured and unstructured data.
-  **Real Time Data:** Dynamic tables can be updated in real-time as new data arrives, so the latest information is available immediately.

**Disadvantages:**

- **Queries:** Querying dynamic tables may require more complex and flexible query logic to handle the changing schema or structure of the data.
- **Data Integrity:** It can be more difficult to ensure data integrity and maintain consistency when dynamic tables' schema can vary, especially when relational constraints are involved.

While materialised views have other strengths, weakness and use cases:

**Advantages:**

- **Performance:** Materialised views are pre-computed and stored copies of data that originate from one or more source tables. They are used to optimise query performance by reducing the need for expensive calculations or query-time joins.

**Disadvantages:**

- **Fixed structure:** Adding or removing columns in materialised views usually requires a rebuild of the view.
- **Data Consistency:** In order to guarantee data consistency with the source tables, materialised views are often refreshed on a frequent basis. The automatic maintenance of materialised views consumes credits.





## Section 1: Preparing Your Data Structure


Create the EMPLOYEE Table and insert the sample data.

```sql
CREATE OR REPLACE TABLE EMPLOYEE (
  EMPLOYEE_ID INT,
  FIRST_NAME VARCHAR(50),
  LAST_NAME VARCHAR(50),
  EMAIL VARCHAR(100),
  PHONE_NUMBER VARCHAR(15),
  FULL_ADDRESS VARCHAR(365),
  UPDATE_TIME TIMESTAMP_NTZ(9)
);
```


```sql
INSERT INTO EMPLOYEE
VALUES
(2, 'Roopa', 'Venkat', 'roopa.venkat@example.com', '0987654321', '456 Pine St, Sydney, NSW, 2170', '2023-05-25 10:00:00'),
(1, 'Jay', 'J', 'jay.j@example.com', '1234567890', '789 Broadway St, Sydney, NSW, 2145', '2023-05-25 11:00:00'),
(3, 'Rene', 'Essomba', 'rene.essomba@example.com', '1122334455', '321 Elm St, NSW, 2150', '2023-05-25 12:00:00');

```


![Employee](section1.png)


## Section 2: Using SQL to Create Dynamic Tables

A dynamic table is created to track the time a record was active and to serve as a substitute key for a specific version of a customer's data. A Type 2 SCD history table, this one. The surrogate key "EMPLOYEE_HISTORY_SK," which is specific to each record in the table, can be used for historical analysis and to join data to a particular version of a customer's information.

```sql
CREATE OR REPLACE DYNAMIC TABLE EMPLOYEE_HISTORY
TARGET_LAG='1 MINUTE'
WAREHOUSE=COMPUTE_WH
AS
SELECT * RENAME (UPDATE_TIME AS RECORD_START_TIME),
EMPLOYEE_ID || '-' || DATE_PART(EPOCH_MILLISECONDS, UPDATE_TIME) AS EMPLOYEE_HISTORY_SK,
SPLIT_PART(FULL_ADDRESS, ' ', -1) AS POSTAL_CODE, 
CASE
    WHEN LEAD(UPDATE_TIME) OVER (PARTITION BY EMPLOYEE_ID ORDER BY UPDATE_TIME ASC) IS NULL THEN '9999-12-31 12:00:00' -- Replace with a valid NULL date representation in your database
    ELSE LEAD(UPDATE_TIME) OVER (PARTITION BY EMPLOYEE_ID ORDER BY UPDATE_TIME ASC)
  END AS LEAD_TIME
FROM EMPLOYEE;
```


A maximum lag of one minute is the aim set for the dynamic table. This ensures that data will never be delayed for longer than one minute, assuming proper warehouse provisioning.

The current row's update time is renamed as "RECORD_START_TIME" column, while the update time for the customer's subsequent chronological row is listed in the "RECORD_END_TIME" column.

"RECORD_END_TIME" is set to max date, indicating that the record includes the most recent information about the client. The dynamic table can also be used to calculate any derived columns, such as "POSTAL_CODE" in this example.


![Employee History](section2.png)


## Section 3 : Update the source records and verify the dynamic table

```sql
INSERT INTO EMPLOYEE
VALUES
(1, 'Jay', 'Jayaram', 'jay.jayaram@example.com', '1234567890', '789 Broadway St, Sydney, NSW, 2160', '2023-05-25 15:00:00')
```


Source Result:


![The flower](sourceresult.png)

Target Result:


![Target Result](targetresult.png)




## Streams and Tasks vs Dynamic Tables
[Streams and tasks versus dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-comparison)

With dynamic tables, data is received and altered from the base tables in response to a written query that specifies the desired outcome. The frequency of these refreshes is determined by an automatic refresh procedure, ensuring that the data satisfies the set freshness (lag) target. While joins, aggregations, window functions, and other SQL constructs are permissible in the SELECT statement for dynamic tables, calls to stored procedures, tasks, UDFs, or external functions are not permitted.

However, Streams and Tasks provide additional flexibility. You can use Python, Java, or Scala-written user-defined functions (UDFs), user-defined table functions (UDTFs), stored procedures, external functions, and Snowpark transformations. 

Furthermore, streams and tasks offer flexibility in managing dependencies and scheduling, which makes them perfect for handling real-time data synchronisation, notifications, and task triggering with complete process control.

Check out my previous blog to see how [Stream and Task can be implemented](https://jayaananthdevops.github.io/posts/snowpipe/)

Dynamic tables are best used when:

In certain circumstances, it could be required to process data where its structure needs to be flexible or doesn't fit into a predetermined format. Dynamic tables come into play in this situation. Dynamic tables provide you more control over handling changing or unpredictable data structures.

[Use cases for dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about#when-to-use-dynamic-tables) include the following:

Removes the need to manage data refreshes, track data dependencies, or write one's own code.
Reduces the complexity of stream and job.
Doesn’t require precise control over the refresh schedule.
Materialisation of the results of a query of multiple base tables.
To summarise, materialised views are fantastic for improving the efficiency of repeated queries, but dynamic tables are perfect for building SQL-based data pipelines. A great option for tracking and controlling real-time data changes is streams and tasks.


## Conclusion

In this post, we have discussed how to create the dynamic table and implement SCD Type 2 using it. With dynamic tables, managing SCDs becomes simpler and more efficient compared to [traditional methods](https://community.snowflake.com/s/article/Building-a-Type-2-Slowly-Changing-Dimension-in-Snowflake-Using-Streams-and-Tasks-Part-1).


Note: This article was originally published on [ Cevo Australia’s website ](https://cevo.com.au/post/dynamic-table-usage-in-snowflake-implementing-type-2-slowly-changing-dimensions-scd-with-flexibility-and-efficiency/)


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

<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-4606733459883553"
     crossorigin="anonymous"></script>