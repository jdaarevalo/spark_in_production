#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import pyspark.sql.functions as func

import pandas as pd


# In[2]:


# create an spark session
spark_conf = SparkConf().setAppName('my_etl')
#spark_conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3,org.postgresql:postgresql:9.4.1211')
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

sc = spark.sparkContext


# In[3]:


df_categories = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load("../data/categories.csv")


# In[11]:


df_categories.printSchema()


# In[5]:


df_cities = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load("../data/cities.csv")


# In[10]:


df_cities.printSchema()


# In[6]:


df_groups = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load("../data/groups.csv")


# In[13]:


df_groups.printSchema()


# In[12]:


df_groups_city = df_groups.join(df_cities, ["city_id"], how="left_outer")


# In[32]:


df_city_groups = df_groups.select('city','group_id')


# In[35]:


df_summary_city_groups = df_city_groups.groupBy('city').count()


# In[37]:


dfp_summary = df_summary_city_groups.toPandas()


# In[46]:


dfp_summary.sort_values('count', ascending=False)


# In[47]:


dfp_summary.to_csv("../output_data/summary_city_groups.csv", index=False)


# In[ ]:




