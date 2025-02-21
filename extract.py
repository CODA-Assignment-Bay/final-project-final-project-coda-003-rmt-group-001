import pyspark
from pyspark.sql import SparkSession

def extract_data(input_path_gni: str, input_path_gdp: str, input_path_edu: str, input_path_homi: str):
    """
    Fungsi ini ditujukan untuk ekstrak data dari CSV.
    
    Parameter:
    input_path_gni: Path file CSV untuk dataset GNI.
    input_path_gdp: Path file CSV untuk dataset GDP.
    input_path_edu: Path file CSV untuk dataset Education.
    input_path_homi: Path file CSV untuk dataset Homicide.
    
    Return:
    dfgni, dfgdp, dfedu, dfhomi: DataFrames untuk masing-masing dataset.
    """
    #Buat SparkSession
    spark = SparkSession.builder.appName("extract_data").getOrCreate()

    output_gni = "/opt/airflow/dags/gni_raw"
    output_homi = "/opt/airflow/dags/homi_raw"
    output_gdp = "/opt/airflow/dags/gdp_raw"
    output_edu = "/opt/airflow/dags/edu_raw"
    
    #Extract data GNI
    dfgni = spark.read.csv(input_path_gni, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    #Extract data GDP
    dfgdp = spark.read.option("delimiter", ";").csv(input_path_gdp, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    #Extract data Education
    dfedu = spark.read.csv(input_path_edu, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    #Extract data Homicide
    dfhomi = spark.read.option("delimiter", ",").csv(input_path_homi, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")

    # Load data GNI
    dfgni.repartition(1).write.csv(output_gni, header=True, mode="overwrite")
    dfgdp.repartition(1).write.csv(output_gdp, header=True, mode="overwrite")
    dfedu.repartition(1).write.csv(output_edu, header=True, mode="overwrite")
    dfhomi.repartition(1).write.csv(output_homi, header=True, mode="overwrite")

#Extract data
extract_data(
    '/opt/airflow/dags/GNI_data.csv', 
    '/opt/airflow/dags/GDP_data_new.csv', 
    '/opt/airflow/dags/Global_Education (1).csv', 
    '/opt/airflow/dags/intentional_homicide.csv'
)