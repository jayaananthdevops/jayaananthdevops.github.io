---
title: SPARK Interview Questions Beginner PART-1
author: jay
date: 2023-11-30 12:00:00 +/-0800
categories: [Interview Questions]
tags: [spark,dataengineer,interviewquestions]     # TAG names should always be lowercase
image:
  path: /assets/interviewquestion/sparkinterview.png
  alt: Spark Interview Questions Beginner
comments: true
---

## 1- What is Lazy Evaluation in Apache Spark??

Before diving into the intricacies of lazy evaluation, it's crucial to grasp two fundamental concepts in Spark: Transformation and Action.

**Transformation:**
In the realm of Spark, the core data structures are immutable, meaning they cannot be altered once created. Transformations are the instructions for modifying these core data structures to shape the DataFrame into the desired form.
A Spark operation that reads a DataFrame, manipulates some of the columns, and returns another DataFrame is referred to as a transformation. Noteworthy is the fact that transformations are evaluated in a lazy fashion. Regardless of the number of scheduled transformations, no Spark jobs are triggered until an action is invoked. Examples of transformations include map(), filter(), groupByKey(), reduceByKey(), join(), and union().

**Action:**
Spark actions are operations that prompt a Spark job to compute and return a result to the Spark driver program or write data to an external storage system. Unlike Spark transformations, which only define a computation path without execution, actions enforce Spark to compute and produce a tangible result. Examples of actions include count, collect, sum, max, min, and foreach.

**Lazy Evaluation:**
Lazy evaluation is a cornerstone feature of Apache Spark that enhances its efficiency and performance. It involves postponing the execution of transformations on distributed datasets until an action is called. This strategy ensures that Spark only processes data when absolutely necessary, leading to significant performance improvements.
When performing operations on RDDs/DataFrames/DataSets, such as filtering or mapping, Spark refrains from immediate data processing. Instead, it constructs a logical execution plan, known as the Directed Acyclic Graph (DAG), which represents the sequence of transformations to be applied incrementally.

The evaluation of the DAG commences only when an action is invoked. Some examples of actions in Spark include collect, count, saveAsTextFile, first, foreach, and countByKey.

**Advantages of Lazy Evaluation:**
* Optimization
* Reduced Disk I/O and Memory Usage

In conclusion, lazy evaluation is a powerful feature of Apache Spark that significantly bolsters its performance and efficiency. Understanding this mechanism is essential for harnessing the full potential of Spark's distributed data processing capabilities.



**If you enjoy the article, Please Subscribe.**

## If you love the article, Please consider supporting me by buying a coffee for $1.


<script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="jayaananth" data-color="#FFDD00" data-emoji="☕"  data-font="Cookie" data-text="Buy me a coffee @ 1$" data-outline-color="#000000" data-font-color="#000000" data-coffee-color="#ffffff" ></script>


<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-4606733459883553"
     crossorigin="anonymous"></script>