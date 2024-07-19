from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('readme.md').getOrCreate()
logfile = "C:\spark\spark-3.5.1-bin-hadoop3\README.md"
logdata = spark.read.text(logfile).cache()

numAs = logdata.filter(logdata.value.contains('a')).count()
numBs = logdata.filter(logdata.value.contains('b')).count()

print(f"number of A's {numAs} and numbers of B's {numBs}")

