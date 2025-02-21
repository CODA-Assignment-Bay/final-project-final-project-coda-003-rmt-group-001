from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

dim_country = spark.read.csv('/opt/airflow/dags/dim_country.csv/*.csv', header=True, inferSchema=True)
fact_education = spark.read.csv('/opt/airflow/dags/fact_education.csv/*.csv', header=True, inferSchema=True)
fact_gdp = spark.read.csv('/opt/airflow/dags/fact_gdp.csv/*.csv', header=True, inferSchema=True)
fact_homi = spark.read.csv('/opt/airflow/dags/fact_homi.csv/*.csv', header=True, inferSchema=True)

print(dim_country.count())
print(fact_education.count())
print(fact_gdp.count())
print(fact_homi.count())

# PostgreSQL JDBC Connection
postgres_url = "jdbc:postgresql://ep-snowy-art-a17wausj-pooler.ap-southeast-1.aws.neon.tech:5432/group001"
postgres_properties = {
    "user": "neondb_owner",
    "password": "s9URtWjSK6IT",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL
dim_country.write.jdbc(url=postgres_url, table="dim_country", mode="overwrite", properties=postgres_properties)
fact_education.write.jdbc(url=postgres_url, table="fact_education", mode="overwrite", properties=postgres_properties)
fact_gdp.write.jdbc(url=postgres_url, table="fact_gdp", mode="overwrite", properties=postgres_properties)
fact_homi.write.jdbc(url=postgres_url, table="fact_homicide", mode="overwrite", properties=postgres_properties)