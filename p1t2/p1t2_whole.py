## initial Setup
import re
import regex
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, lower
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
## df
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_whole.xml')


## internal link filter function
def get_internal_link(text):
    matches = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text)
    result = []
    for match in matches:
        splited = re.split(r'\|', match)
        temp = []
        for link in splited:
            if len(re.findall(r'^category:', link)) >0:
                temp.append(link)
            elif len(re.findall(r':|#', link)) > 0:
                continue
            else:
                temp.append(link)
        if len(temp)>0 :
            result.append(temp[0]) 
        else:
            continue
    return result

get_internal_link = udf(get_internal_link, ArrayType(StringType()))


## map reduce function
def process(df):
    output = df.select(lower(df.title).alias('title'), lower(df.revision.text._Value).alias('text')) \
        .dropna(how='any') \
        .withColumn('internal_link', get_internal_link('text')) \
        .select('title', explode('internal_link').alias('internal_link')) \
        .sort('title', 'internal_link')
    return output

whole_result = process(df)
whole_result.coalesce(1).write.option("delimiter", "\t").csv("gs://hw2-backup/p1t2_whole.csv")