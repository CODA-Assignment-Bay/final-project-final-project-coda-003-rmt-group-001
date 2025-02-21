import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, concat, concat_ws, lit, to_date, regexp_replace

def transform_data(dfgni_path: str, dfgdp_path :str, dfedu_path: str,dfhomi_path :str):
    """
    Fungsi ini untuk melakukan transformasi data Spark DataFrame.
    Transformasi yang dilakukan:
     - Mengubah nama kolom menjadi lowercase dan menggunakan snakecase
    - Melakukan melt pada kolom dengan stack
    - Menambah kolom disesuaikan dengan tabel fact dan dim
    - Filter kategori untuk kemudian dimasukkan ke kolom baru
    
    Parameter:
    dfgni_path: DataFrame untuk dataset GNI.
    dfgdp_path: DataFrame untuk dataset GDP.
    dfedu_path: DataFrame untuk dataset Education.
    dfhomi_path: DataFrame untuk dataset Homicide.
    
    Return:
    dim_country, fact_education, fact_gdp, fact_homi: DataFrames yang telah ditransformasi.
    """
    
    #Buat SparkSession
    spark = SparkSession.builder.appName("transform_data").getOrCreate()

    # Baca data yang diekstrak
    dfgni = spark.read.csv(dfgni_path, header=True, inferSchema=True)
    dfgdp = spark.read.csv(dfgdp_path, header=True, inferSchema=True)
    dfedu = spark.read.csv(dfedu_path, header=True, inferSchema=True)
    dfhomi = spark.read.csv(dfhomi_path, header=True, inferSchema=True)
    
    #TRANSFORMASI
    
    #Melakukan melt pada kolom "years" dengan stack
    years = [1960,1961,1962,1963,1964,1965,1966,1967,
             1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,
             1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,
             1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,
             2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]
    years = [str(x) for x in years]

    #Transformasi GNI DataFrame
    #Buat list kosong untuk menyimpan yang telah dimodifikasi
    melted_df_list = []
    #Memproses data untuk setiap tahun yang ada di variabel 'years'
    for year in years:
        melted_df = dfgni.select(
            'Country Name',
            'Country Code',
            col(year).alias('GNI'),
            lit(year).alias('Year')
        )
    #Masukkan DataFrame yang sudah dimodifikasi ke dalam list
        melted_df_list.append(melted_df)
    #Membuat DataFrame awal sebagai hasil akhir
        final_dfgni = melted_df_list[0]
    #Gabungkan DataFrame lainnya dengan menggunakan union
    for melted_df in melted_df_list[1:]:
        final_dfgni = final_dfgni.union(melted_df)
    #Rename semua kolom: lowercase dan menggunakan snakecase
    final_dfgni = final_dfgni.toDF(*[col.lower().replace(" ", "_") for col in final_dfgni.columns])
    #Mengganti nama kolom "country_name" menjadi "country"
    final_dfgni = final_dfgni.withColumnRenamed("country_name", "country")
    #Membuat kolom baru dan mengisinya dengan mengelompokan conflict zone, high, middle, dan low income
    final_dfgni = final_dfgni.withColumn("country_status", when(col("country").isin(["Afghanistan", "Burkina Faso",
                                                                                     "Cameroon", "Africa", "Ethiopia",
                                                                                     "Haiti", "Iraq", 
                                                                                     "Lebanon", "Mozambique", "Myanmar",
                                                                                     "Niger", "Nigeria", "Somalia",
                                                                                     "South Sudan", "Syrian Arab Republic", "Ukraine", "Yemen"]), "Conflict Zone")
                                         .when(col("GNI") > 13846, "High Income")
                                         .when((col("GNI") > 1136) & (col("GNI") < 13845), "Middle Income")
                                         .otherwise("Low Income")
                                        )

    #Transformasi GDP DataFrame
    #Buat list kosong untuk menyimpan yang telah dimodifikasi
    melted_df_list = []
    #Memproses data untuk setiap tahun yang ada di variabel 'years'
    for year in years:
        melted_df = dfgdp.select(
            'Country Name',
            'Country Code',
            col(year).alias('GDP'),
            lit(year).alias('Year')
        )
    #Masukkan DataFrame yang sudah dimodifikasi ke dalam list
        melted_df_list.append(melted_df)
    #Membuat DataFrame awal sebagai hasil akhir
        final_dfgdp = melted_df_list[0]
    #Gabungkan DataFrame lainnya dengan menggunakan union
    for melted_df in melted_df_list[1:]:
        final_dfgdp = final_dfgdp.union(melted_df)
    #Rename semua kolom: lowercase dan menggunakan snakecase
    final_dfgdp = final_dfgdp.toDF(*[col.lower().replace(" ", "_") for col in final_dfgdp.columns])
    final_dfgdp = final_dfgdp.withColumnRenamed("country_name","country")

    #Transformasi Education DataFrame
    #Menyatukan 2 kolom dalam 1 kolom baru
    dfedu = dfedu.withColumn("Dropout_Rate", (col("OOSR_Upper_Secondary_Age_Male") +
                                              col("OOSR_Upper_Secondary_Age_Female")) / 2)
    dfedu = dfedu.withColumn("Completion_Rate", (col("Completion_Rate_Upper_Secondary_Male") +
                                                 col("Completion_Rate_Upper_Secondary_Female")) / 2)
    #Memilih kolom yang dibutuhkan
    final_dfedu = dfedu.select(
        'Countries and areas',
        'Latitude',
        'Longitude',
        'Dropout_Rate',
        'Completion_Rate',
        'Unemployment_Rate'
    )
    #Rename semua kolom: lowercase dan menggunakan snakecase
    final_dfedu = final_dfedu.toDF(*[col.lower().replace(" ", "_") for col in final_dfedu.columns])
    final_dfedu = final_dfedu.withColumnRenamed("countries_and_areas","country")

    #Transformasi Homicide DataFrame
    #Rename semua kolom: lowercase dan menggunakan snakecase
    dfhomi = dfhomi.toDF(*[col.lower().replace(" ", "_") for col in dfhomi.columns])
    
    #Filter kategori "Counts" dan "Rate per 100000"
    dfcount = dfhomi.filter(col("unit_of_measurement") == "Counts").filter(col("sex") == "Total")\
            .filter(col("category") == "Total").filter(col("indicator") == "Persons convicted for intentional homicide")
    dfrate = dfhomi.filter(col("unit_of_measurement") == "Rate per 100,000 population").filter(col("sex") == "Total")\
            .filter(col("category") == "Total").filter(col("indicator") == "Persons convicted for intentional homicide")
    #Rename data yang telah difilter
    dfcount = dfcount.withColumnRenamed("value","number_convicted_for_intentional_homicide")
    dfrate = dfrate.withColumnRenamed("value","rate_per_100k_population")
    #Menyatukan data yang telah difilter kedalam DataFrame
    final_dfhomi = dfcount.join(dfrate, on=["country","region","subregion","year"], how="left")\
                    .select(
                        dfcount.country,
                        dfcount.region,
                        dfcount.subregion,
                        dfcount.year,
                        dfcount.number_convicted_for_intentional_homicide,
                        dfrate.rate_per_100k_population
                    )
    
    #---------------------------------------------------------------------------------------#

    #Buat tabel dim country
    #Memilih kolom 'country', 'latitude', dan 'longitude' dari DataFrame final_dfedu
    dfedu3 = final_dfedu\
                .select(
                    final_dfedu.country,
                    final_dfedu.latitude, final_dfedu.longitude
                )
    #Mengambil DataFrame final_dfgni untuk tahun 2021 dan memilih kolom 'country', 'country_code', dan 'country_status'
    dfgni3 = final_dfgni.filter(col("year") == "2021").select(
        final_dfgni.country, final_dfgni.country_code, final_dfgni.country_status)
    #Memilih kolom 'country', 'region', dan 'subregion' dari DataFrame final_dfhomi
    dfhomi3 = final_dfhomi.select(
        final_dfhomi.country, final_dfhomi.region, final_dfhomi.subregion)
    #Melakukan join DataFrame dfedu3, dfhomi3, dan dfgni3 berdasarkan kolom 'country' dan drop duplikasi
    dim_country = dfedu3.join(dfhomi3, on=["country"], how="left").join(dfgni3, on="country",
                                                                        how="left").dropDuplicates()

    #Buat tabel fact education
    fact_education = final_dfedu.select(
        final_dfedu.country,
        final_dfedu.completion_rate, final_dfedu.dropout_rate,
        final_dfedu.unemployment_rate)
    # Inisiasi Primary Key
    fact_education = fact_education.withColumn("country_year", concat(col("country"), lit("_"), lit("2021")))
    
    #Buat tabel fact GDP
    fact_gdp = final_dfgdp.select(
        final_dfgdp.country, final_dfgdp.year, final_dfgdp.gdp)
    # Inisiasi Primary Key
    fact_gdp = fact_gdp.withColumn("country_year", concat(col("country"),lit("_"),col("year")))
    fact_gdp = fact_gdp.withColumn("gdp", regexp_replace("gdp", ",", "."))



    #Buat tabel fact homicide
    fact_homi = final_dfhomi.select(
        final_dfhomi.country, final_dfhomi.year, 
        final_dfhomi.number_convicted_for_intentional_homicide, final_dfhomi.rate_per_100k_population)
    # Inisiasi Primary Key
    fact_homi = fact_homi.withColumn("country_year", concat(col("country"),lit("_"),col("year")))

    dim_country.repartition(1).write.csv('/opt/airflow/dags/dim_country.csv', header=True, mode="overwrite")
    fact_education.repartition(1).write.csv('/opt/airflow/dags/fact_education.csv', header=True, mode="overwrite")
    fact_gdp.repartition(1).write.csv('/opt/airflow/dags/fact_gdp.csv', header=True, mode="overwrite")
    fact_homi.repartition(1).write.csv( '/opt/airflow/dags/fact_homi.csv', header=True, mode="overwrite")
    
    return dim_country, fact_education, fact_gdp, fact_homi

transform_data(
    "/opt/airflow/dags/gni_raw", 
    "/opt/airflow/dags/gdp_raw", 
    "/opt/airflow/dags/edu_raw", 
    "/opt/airflow/dags/homi_raw"
)