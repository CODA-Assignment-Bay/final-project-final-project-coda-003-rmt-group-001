from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col

#Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()


#PostgreSQL JDBC Connection
postgres_url = "jdbc:postgresql://ep-snowy-art-a17wausj-pooler.ap-southeast-1.aws.neon.tech:5432/group001"
postgres_properties = {
    "user": "neondb_owner",
    "password": "s9URtWjSK6IT",
    "driver": "org.postgresql.Driver"
}

print('aaaaa')

edu = """SELECT
	*
FROM fact_education
"""
fedu = f"({edu}) AS subquery"

fact_education = spark.read.jdbc(
        url=postgres_url, 
        table=fedu, 
        properties=postgres_properties
)

gdp = """SELECT
	*
FROM fact_gdp
"""
fgdp = f"({gdp}) AS subquery"

fact_gdp = spark.read.jdbc(
        url=postgres_url, 
        table=fgdp, 
        properties=postgres_properties
)

homi = """SELECT
	*
FROM fact_homicide
"""
fhomi = f"({homi}) AS subquery"

fact_homicide = spark.read.jdbc(
        url=postgres_url, 
        table=fhomi, 
        properties=postgres_properties
)

country = """SELECT
	*
FROM dim_country
"""
fcountry = f"({country}) AS subquery"

dim_country = spark.read.jdbc(
        url=postgres_url, 
        table=fcountry, 
        properties=postgres_properties
)

fact_education = fact_education.drop("country_year").withColumn("year", lit(2021))
fact_gdp = fact_gdp.drop("country_year")
fact_homicide = fact_homicide.drop("country_year")

print('xxxx')

#Perform LEFT JOIN with df2 and df3 using df1 as the base
datamart = fact_education \
    .join(dim_country,  on=["country"], how="left")\
    .join(fact_gdp, ["country","year"], "left")\
    .join(fact_homicide, ["country","year"], "left")  

print('bbbb')

datamart.write.jdbc(url=postgres_url, table="datamart", mode="overwrite", properties=postgres_properties)