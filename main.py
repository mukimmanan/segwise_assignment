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
    "minInstalls",
    binning_data(col("minInstalls"), lit(100000))
).cache()
# Caching the data since, we will be using this dataframe for multiple actions, so caching will help reduce time

# Creating Temp Views
data.createOrReplaceTempView("playstore")

# Insights
output = spark.sql("""
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
    CONCAT('Year=', releasedYear, ';', ' Score=[', score, ']') AS metric,
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
    CONCAT('Year=', releasedYear, ';', ' Rating=[', ratings, ']') AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    ratings 

UNION ALL

SELECT 
    CONCAT('Year=', releasedYear, ';', ' Score=[', score, ']') AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    releasedYear,
    score 

UNION ALL

SELECT 
    CONCAT('Free=', free, ';', ' OffersIAP=', offersIAP) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    free,
    offersIAP 

UNION ALL

SELECT 
    CONCAT('AdSupported=', adSupported, ';', ' ContainsAds=', containsAds) AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    adSupported,
    containsAds

UNION ALL

SELECT 
    CONCAT('Free=', free, ';', ' Reviews=[', reviews, ']') AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    free,
    reviews

UNION ALL

SELECT 
    CONCAT('OffersIAP=', offersIAP, ';', ' Reviews=[', reviews, ']') AS metric,
    count(appId) AS value
FROM playstore
GROUP BY
    offersIAP,
    reviews
""")

filter_data_perc = data.count() * 0.02
output.filter("LENGTH(metric) > 0 AND value > {}".format(filter_data_perc)).coalesce(1).write.mode("overwrite").csv(path="./output/", header=True)
