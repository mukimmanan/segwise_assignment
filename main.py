from pyspark.sql.functions import *
from pyspark.sql.context import SparkContext
from pyspark.sql.session import SparkSession
from utils import csv_read, bin_data
from pyspark.sql.types import *

sc = SparkContext(appName="Segwise Assignment", master="local[*]")
spark = SparkSession(sc).builder.getOrCreate()

# Registering UDFs
binning_data = udf(lambda val, num: bin_data(val, num), StringType())

data = sc \
    .textFile("./playstore.csv") \
    .mapPartitions(lambda row: csv_read(row)) \
    .map(lambda row: row[1:] if len(row) == 36 else row) \
    .toDF([
        'appId', 'developer', 'developerId', 'developerWebsite', 'free', 'genre', 'genreId', 'inAppProductPrice',
        'minInstalls', 'offersIAP', 'originalPrice', 'price', 'ratings', 'len screenshots', 'adSupported',
        'containsAds', 'reviews', 'releasedDayYear', 'sale', 'score', 'summary', 'title', 'updated',
        'histogram1', 'histogram2', 'histogram3', 'histogram4', 'histogram5', 'releasedDay',
        'releasedYear', 'releasedMonth', 'dateUpdated', 'minprice', 'maxprice', 'ParseReleasedDayYear'
    ])

data = data.select(
    "appId",
    "developerId",
    col("free").cast(BooleanType()),
    "genreId",
    col("minInstalls").cast(IntegerType()),
    "offersIAP",
    col("price").cast(DoubleType()),
    col("ratings").cast(IntegerType()),
    col("len screenshots").cast(IntegerType()),
    col("adSupported").cast(BooleanType()),
    col("containsAds").cast(BooleanType()),
    col("reviews").cast(IntegerType()),
    col("releasedDayYear"),
    col("score").cast(FloatType()),
    col('releasedYear').cast(IntegerType()),
    'releasedMonth',
    'dateUpdated',
    'ParseReleasedDayYear'
)

data = data.filter("appId != 'appId'")

data.summary("count", "min", "max").show(truncate=False)
data = data.withColumn(
    "price",
    binning_data(col("price"), lit(50))
).withColumn(
    "ratings",
    binning_data(col("ratings"), lit(10000))
).withColumn(
    "reviews",
    binning_data(col("reviews"), lit(10000))
).withColumn(
    "score",
    binning_data(col("score"), lit(1))
).withColumn(
    "score",
    binning_data(col("minInstalls"), lit(100000))
).cache()

# Creating Temp Views
data.createOrReplaceTempView("playstore")

# Insights
spark.sql("""
SELECT 
    CONCAT('Year=', releasedYear) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear
    
UNION ALL

SELECT 
    CONCAT('Free=', free, ';', ' Genre=', genreId, ';', ' Year=', releasedYear) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    free,
    genreId,
    releasedYear
    
UNION ALL

SELECT
    CONCAT('Year=', releasedYear, ';', ' score=', score) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    score

UNION ALL

SELECT 
    CONCAT('Price=[', price, '];', ' Genre=', genreId, ';', ' Installs=[', minInstalls, ']') AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    price,
    genreId,
    minInstalls

UNION ALL

SELECT 
    CONCAT('Year=', releasedYear, ';', ' AdSupported=', adSupported) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    adSupported

UNION ALL

SELECT 
    CONCAT('Year=', releasedYear, ';', ' Rating=', ratings) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    ratings 

UNION ALL

SELECT 
    CONCAT('Year=', releasedYear, ';', ' Score=', score) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    score 

UNION ALL

SELECT 
    CONCAT('Free=', free, ';', ' IAP=', offersIAP) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    free,
    offersIAP 

""").write.csv(path=)