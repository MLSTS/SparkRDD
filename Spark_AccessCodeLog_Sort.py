from pyspark.sql import SparkSession

if __name__ == "__main__":
        spark = SparkSession.builder.getOrCreate()

        sc = spark.sparkContext
        lines = sc.textFile("/home/student/CSS333/dataset/accesslog.csv",4)
        codes = lines.map(lambda str: (str.split(",")[7],1))

        counts = codes.reduceByKey(lambda i,j: i+j)
        sorted = counts.sortBy(lambda x: x[1], ascending=False, numPartitions=1)

        output = sorted.collect()
        for (code, count) in output:
                print("%s: %i" %(code, count))

        spark.stop()
