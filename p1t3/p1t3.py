#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, lower, col, lit
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()


# In[2]:


df = spark.read.csv('gs://hw2-jl-bucket/p1t20428.csv', sep = '\t')


# In[3]:


def add_num_neighbors(df):
    num_neighbor = df.groupBy('_c0').count()
    df = df.join(num_neighbor, df['_c0'] == num_neighbor['_c0'], how='left').select(df['*'], num_neighbor['count'])
    return(df)


# In[6]:


def PageRank(df):
    
    df = df.withColumn('contribute', df['rank']/df['count'])
#     df.show()
    
    total_contribution = df.groupBy('_c1').sum('contribute')
    total_contribution = total_contribution.select(col('_c1').alias('c1'), col('sum(contribute)').alias('contribute_sum'))
    df = df.join(total_contribution, df['_c0'] == total_contribution['c1'], how='left').select(df['*'], total_contribution['contribute_sum'])
#     df.show()
    
    df = df.withColumn( 'new_rank', df['contribute_sum'] * (17/20) + 3/20 )
#     df.show()

    df = df.select('_c0', '_c1', 'count', col('new_rank').alias('rank'))
#     df.show()
    
    return(df)


# In[7]:


df = add_num_neighbors(df).withColumn('rank', lit(1))
df.show()

for it in range(10):
    df = PageRank(df)

df.show()


# In[8]:


df = df.select(col('_c0').alias('article'), 'rank').distinct().dropna().sort(["article", "rank"],ascending=True).limit(5)
df.show()


# In[9]:


gcs_filepath = 'gs://hw2-jl-bucket/p1t3_small_final.csv'
df.coalesce(1).write.option("delimiter", "\t").csv(gcs_filepath)

