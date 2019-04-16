from pyspark.sql import SparkSession

if __name__ == "__main__":
        spark = SparkSession.builder.getOrCreate()

        sc = spark.sparkContext
        lines = sc.textFile("/home/student/CSS333/dataset/accesslog.csv",4)
        ips = lines.map(lambda str: (str.split(",")[0],1))

        counts = ips.reduceByKey(lambda i,j: i+j)
        sorted = counts.sortBy(lambda x: x[1], ascending=False, numPartitions=1)

        output = sorted.collect()
        for (ip, count) in output[:10]:
                print("%s: %i" %(ip, count))

        spark.stop()
