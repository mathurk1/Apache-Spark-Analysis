from pyspark.sql import SparkSession
from pyspark.sql import Row
import re

#### Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

p = re.compile(
    '([^ ]*) ([^ ]*) ([^ ]*) \[([^]]*)\] "([^"]*)" ([^ ]*) ([^ ]*)'
    )

def mapper_parser(line):
    fields = p.match(line)
    field_name = list(fields.groups())

    return Row(host=field_name[0],
               ignore=field_name[1],
               user=field_name[2],
               date=field_name[3],
               request=field_name[4],
               status=field_name[5],
               size=field_name[6])

lines = spark.sparkContext.textFile("access_log_sample.txt")
matchedLines = lines.map(mapper_parser)

#### Infer the schema, and register the DataFrame as a table.
schemaLog = spark.createDataFrame(matchedLines).cache()
schemaLog.createOrReplaceTempView("WebServerLogs")

#### SQL can be run over DataFrames that have been registered as a table.

# number of page not found return status
pageNotFoundStatus = spark.sql("SELECT COUNT(*) AS COUNT FROM WebServerLogs WHERE status = 404")
for results in pageNotFoundStatus.collect():
    print(results)

# number of hits per IP address
schemaLog.groupBy("host").count().show()

spark.stop()
