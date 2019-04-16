from pyspark.sql import SparkSession

if __name__ == "__main__":
        spark = SparkSession.builder.getOrCreate()

        sc = spark.sparkContext
        dataRdd = sc.textFile("/home/student/CSS333/dataset/randoms.txt",2)
        newRdd = dataRdd.map(lambda str: int(str))

        min = newRdd.reduce(lambda i,j: i if (i<j) else j)
        max = newRdd.reduce(lambda i,j: i if (i>j) else j)

        print("Minimum number: %d" % min)
        print("Maximum number: %d" % max)

        spark.stop()
