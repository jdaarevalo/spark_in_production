#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
import os

#import findspark
#findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import pyspark.sql.functions as func

import pandas as pd


# In[2]:


BUCKET_S3 = os.environ.get('BUCKET_S3','')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID','')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY','')


# In[3]:


path_categories = "{}/data/categories.csv".format(BUCKET_S3.replace('s3','s3a'))
path_cities = "{}/data/cities.csv".format(BUCKET_S3.replace('s3','s3a'))
path_groups = "{}/data/groups.csv".format(BUCKET_S3.replace('s3','s3a'))


# In[4]:


# create an spark session
spark_conf = SparkConf().setAppName('my_etl')
spark_conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0')
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

sc = spark.sparkContext


# In[5]:


sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") 
sc._jsc.hadoopConfiguration().set("fs.s3n.multiobjectdelete.enable","false")

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)


# In[6]:


df_categories = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load(path_categories)


# In[7]:


df_categories.printSchema()


# In[8]:


df_cities = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load(path_cities)


# In[9]:


df_cities.printSchema()


# In[10]:


df_groups = spark.read.format("csv")                            .option("header", "true")                            .option("inferSchema", "true")                            .load(path_groups)


# In[11]:


df_groups.printSchema()


# In[12]:


df_groups_city = df_groups.join(df_cities, ["city_id"], how="left_outer")


# In[13]:


df_city_groups = df_groups.select('city','group_id')


# In[14]:


df_summary_city_groups = df_city_groups.groupBy('city').count()


# In[23]:


path_to_write = "{}/output_data/summary_city_groups".format(BUCKET_S3.replace('s3','s3a'))


# In[24]:


df_summary_city_groups.write.parquet(path_to_write)


# In[ ]:





# In[15]:


dfp_summary = df_summary_city_groups.toPandas()


# In[16]:


dfp_summary.sort_values('count', ascending=False)


# In[ ]:


dfp_suma


# In[ ]:


dfp_summary.to_csv("../output_data/summary_city_groups.csv", index=False)


# In[ ]:




