from pyspark.sql import SparkSession

if __name__ == "__main__":
        spark = SparkSession.builder.getOrCreate()

        sc = spark.sparkContext
        dataRdd = sc.textFile("/home/student/CSS333/dataset/randoms.txt",2)
        newRdd = dataRdd.map(lambda str: int(str))

        sortRdd = newRdd.sortBy(lambda x: x, ascending=True, numPartitions=1)

        sortRdd.saveAsTextFile("/home/student/css333/example/sorted")

        spark.stop()
