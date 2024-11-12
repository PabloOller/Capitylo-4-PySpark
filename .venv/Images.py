from pyspark.ml import image
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Imagenes") \
    .getOrCreate()


# Reading an image le into a DataFrame

image_dir = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select("image.height", "image.width", "image.nChannels", "image.mode",
 "label").show(5, truncate=False)

# Reading a binary file into a DataFrame

path = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .load(path))
binary_files_df.show(5)


binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .option("recursiveFileLookup", "true")
 .load(path))
binary_files_df.show(5)

spark.stop()
