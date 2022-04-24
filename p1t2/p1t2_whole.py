## initial Setup
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, lower
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
## df
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_whole.xml')


## internal link filter function
def get_internal_link(text):
    matches = re.findall(r'\[\[(.*?)\]\]', text)
    for i, match in enumerate(matches):
        matches[i] = re.split('\|', match)[0]

    result = []
    for match in matches:
        if len(re.findall(r'^category:', match)) > 0:
            result.append(match)
        elif len(re.findall(r':|#', match)) > 0:
            continue
        else:
            result.append(match)
    return result

get_internal_link = udf(get_internal_link, ArrayType(StringType()))


## map reduce function
def process(df):
    output = df.select(lower(df.title).alias('title'), lower(df.revision.text._Value).alias('text')) \
        .dropna(how='any') \
        .withColumn('internal_link', get_internal_link('text')) \
        .select('title', explode('internal_link').alias('internal_link')) \
        .sort('title', 'internal_link') \
        .limit(5)
    return output

whole_result = process(df)
whole_result.coalesce(1).write.option("delimiter", "\t").csv("p1t2_whole.csv")