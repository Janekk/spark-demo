from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id, quarter, year, month, dayofmonth, date_format, col
from pyspark.sql import Row
from dateutil.rrule import DAILY, rrule
from datetime import date


def load_datasources(spark):
    # Load JDBC data sources (tables: claims, drugproduct, drugproductfamily, indication):
    jdbc_url = 'jdbc:postgresql://localhost:5432/bigdata_source'
    jdbc_table_claim = 'public.claim'
    jdbc_table_drugproduct = 'public.drugproduct'
    jdbc_table_drugproductfamily = 'public.drugproductfamily'
    jdbc_table_indication = 'public.indication'
    jdbc_credentials = {'user': 's45045', 'password': 'test'}
    data_filepath_base = '/home/janusz/bigdata/projekt/data/'

    src_claims = spark.read \
        .jdbc(jdbc_url, jdbc_table_claim,
              properties=jdbc_credentials)

    src_drugproduct = spark.read \
        .jdbc(jdbc_url, jdbc_table_drugproduct,
              properties=jdbc_credentials)

    src_drugproductfamily = spark.read \
        .jdbc(jdbc_url, jdbc_table_drugproductfamily,
              properties=jdbc_credentials)

    src_indication = spark.read \
        .jdbc(jdbc_url, jdbc_table_indication,
              properties=jdbc_credentials)

    # Load file data sources:
    src_countries = spark.read.load(data_filepath_base + 'countries_data.csv', format='csv', header='true')

    src_factories1 = spark.read.load(data_filepath_base + 'factories.tsv', format='csv', delimiter='\t')
    src_factories2 = spark.read.load(data_filepath_base + 'factories_2.tsv', format='csv', delimiter='\t')
    src_factories = src_factories1.unionAll(src_factories2).distinct()
    src_factories = src_factories.toDF('code', 'name', 'country')

    src_responses1 = spark.read.load(data_filepath_base + 'responses_data.csv', format='csv', header='true')
    src_responses2 = spark.read.load(data_filepath_base + 'responses_data_2.csv', format='csv', header='true')
    src_responses = src_responses1.unionAll(src_responses2).distinct() \
        .toDF('claim_number', 'response_date', 'response')

    # Build Facts and Dimensions data frames
    def format_bk(bk):
        return bk.strip().upper()

    udf_format_bk = udf(format_bk)
    spark.udf.register('format_bk', format_bk)

    dim_countries = src_countries \
        .toDF('country_name', 'alpha_2_code', 'alpha_3_code', 'numeric_code', 'link') \
        .select('alpha_2_code', 'country_name') \
        .toDF('country_bk', 'country_name') \
        .withColumn('country_id', monotonically_increasing_id())

    dim_factories = src_factories \
        .withColumn('code', udf_format_bk(src_factories.code)) \
        .toDF('factory_bk', 'factory_name', 'factory_country') \
        .withColumn('factory_id', monotonically_increasing_id())

    dim_indications = src_indication \
        .select('indicationid', 'indicationdescription') \
        .toDF('indication_bk', 'indication_description') \
        .withColumn('indication_id', monotonically_increasing_id())

    dic_response_code = spark.read.load(data_filepath_base + 'dic_response_code.tsv', format='csv', delimiter='\t') \
        .toDF('from', 'to')
    src_responses.createOrReplaceTempView('response_src')
    dic_response_code.createOrReplaceTempView('dic_response_code')
    dim_responses = spark.sql("""
        SELECT
        monotonically_increasing_id() as response_id,
        format_bk(claim_number) as response_bk,
        response_date,
        dic.to as response
        FROM response_src
        INNER JOIN dic_response_code as dic ON response_src.response = dic.from
        """)

    src_drugproduct.createOrReplaceTempView('src_drugproduct')
    src_drugproductfamily.createOrReplaceTempView('src_drugproductfamily')

    dim_products = spark.sql("""
    SELECT monotonically_increasing_id() as product_id,
    format_bk(drugproductid) as product_bk,
    drugproductname as product_name,
    drugproductfamilyname as product_family_name
    FROM src_drugproduct
    LEFT OUTER JOIN src_drugproductfamily ON src_drugproduct.drugproductfamilyid = src_drugproductfamily.drugproductfamilyid""")

    format_quarter = udf(lambda num_quarter: 'Q' + str(num_quarter))

    calendar_start = date(year=1900, month=1, day=1)
    calendar_end = date(year=2100, month=1, day=1)
    calendar_rdd = spark.sparkContext.parallelize(
        [dt.date() for dt in rrule(freq=DAILY, dtstart=calendar_start, until=calendar_end)]) \
        .map(lambda dt: Row(dt=dt))

    dim_calendar = spark.createDataFrame(calendar_rdd)
    dim_calendar = dim_calendar \
        .withColumn('date_id', monotonically_increasing_id()) \
        .withColumn('date', date_format(dim_calendar.dt, 'YYYY-MM-dd')) \
        .withColumn('year', year(dim_calendar.dt)) \
        .withColumn('month', month(dim_calendar.dt)) \
        .withColumn('day', dayofmonth(dim_calendar.dt)) \
        .withColumn('quarter', quarter(dim_calendar.dt))

    dim_calendar = dim_calendar \
        .withColumn('quarter', format_quarter(dim_calendar.quarter))

    src_claims.cache()
    dim_responses.cache()
    dim_calendar.cache()
    dim_countries.cache()
    dim_factories.cache()
    dim_indications.cache()
    dim_products.cache()

    src_claims.createOrReplaceTempView('claim')
    dim_responses.createOrReplaceTempView('response')
    dim_calendar.createOrReplaceTempView('calendar')
    dim_countries.createOrReplaceTempView('country')
    dim_factories.createOrReplaceTempView('factory')
    dim_indications.createOrReplaceTempView('indication')
    dim_products.createOrReplaceTempView('product')

    fact_registration_claim = spark.sql("""
      SELECT
      format_bk(claimnumber) as claimnumber,
      product.product_id as product_id,
      indication.indication_id as indication_id,
      factory_api.factory_id as factory_api_id,
      factory_bulk.factory_id as factory_bulk_id,
      factory_package.factory_id as factory_package_id,
      calendar_response.date_id as response_date_id,
      calendar_expectedresponse.date_id as expectedresponse_date_id,
      calendar_submission.date_id as submission_date_id,
      country.country_id as country_id,
      response.response_id as response_id
      FROM claim
      LEFT OUTER JOIN indication on indication.indication_bk = claim.indicationid
      LEFT OUTER JOIN product on product.product_bk = claim.productcodeid
      LEFT OUTER JOIN response on response.response_bk = claim.claimnumber
      LEFT OUTER JOIN calendar as calendar_response on calendar_response.date = response.response_date
      LEFT OUTER JOIN calendar as calendar_expectedresponse on calendar_expectedresponse.date = claim.expectedresponsedate
      LEFT OUTER JOIN calendar as calendar_submission on calendar_submission.date = claim.submissiondate
      LEFT OUTER JOIN country on country.country_bk = claim.countrycode
      LEFT OUTER JOIN factory as factory_api on factory_api.factory_bk = claim.factoryapi
      LEFT OUTER JOIN factory as factory_bulk on factory_bulk.factory_bk = claim.factorybulk
      LEFT OUTER JOIN factory as factory_package on factory_package.factory_bk = claim.factorypackage
      WHERE claim.claimtype = 'REGISTER'
      """)

    fact_reimbursement_claim = spark.sql("""
      SELECT
      format_bk(claimnumber) as claimnumber,
      product.product_id as product_id,
      reimbursementprct as reimbursement_claimed_prc,
      reimbursementprct as reimbursement_app_prc,
      indication.indication_id as indication_id,
      calendar_response.date_id as response_date_id,
      calendar_expectedresponse.date_id as expectedresponse_date_id,
      calendar_submission.date_id as submission_date_id,
      country.country_id as country_id,
      response.response_id as response_id,
      CAST(TRIM(SUBSTRING(claim.drugprice, 1, LENGTH(claim.drugprice)-3)) AS INT) AS drug_price,
      SUBSTRING(claim.drugprice, -3, 3) AS drug_price_currency
      FROM claim
      LEFT OUTER JOIN indication on indication.indication_bk = claim.indicationid
      LEFT OUTER JOIN product on product.product_bk = claim.productcodeid
      LEFT OUTER JOIN response on response.response_bk = claim.claimnumber
      LEFT OUTER JOIN calendar as calendar_response on calendar_response.date = response.response_date
      LEFT OUTER JOIN calendar as calendar_expectedresponse on calendar_expectedresponse.date = claim.expectedresponsedate
      LEFT OUTER JOIN calendar as calendar_submission on calendar_submission.date = claim.submissiondate
      LEFT OUTER JOIN country on country.country_bk = claim.countrycode
      WHERE claim.claimtype = 'REFUND'
      AND claim.drugprice IS NOT NULL AND claim.drugprice != 'null'
      """)

    fact_registration_claim.cache()
    fact_reimbursement_claim.cache()
    fact_registration_claim.createOrReplaceTempView('registration_claim')
    fact_reimbursement_claim.createOrReplaceTempView('reimbursement_claim')

    print('1) Liczba wnioskow o rejestracje na rok')
    # question_1 = spark.sql("""
    #         SELECT
    #         count(claimnumber) as number_of_claims, year
    #         FROM reimbursement_claim
    #         INNER JOIN calendar on submission_date_id = calendar.date_id
    #         group by year
    #         order by year
    #     """)

    question_1 = fact_registration_claim.alias('claim') \
        .join(dim_calendar.alias('calendar'), col('claim.submission_date_id') == col('calendar.date_id')) \
        .groupBy(col('calendar.year')) \
        .count() \
        .orderBy(col('calendar.year'))
    question_1.show()

    # print('2.a) Odsetek wnioskow o refundacje pozytywnych rozpatrzonych w terminie')
    # question_2a = spark.sql("""
    #     select
    #       (SELECT count(claimnumber) as pozytywne
    #       FROM reimbursement_claim
    #       INNER JOIN response on reimbursement_claim.response_id = response.response_id
    #       INNER JOIN calendar expected_cal on expectedresponse_date_id = expected_cal.date_id
    #       INNER JOIN calendar response_cal on response_date_id = response_cal.date_id
    #       where response.response = 'Approved'
    #       and response_cal.date <= expected_cal.date)
    #     /
    #     CAST(count(claimnumber) as FLOAT) AS ODSETEK_REFUND
    #     FROM reimbursement_claim
    #     """)
    # question_2a.show()

    question_2a = fact_reimbursement_claim.alias('claim') \
                      .join(dim_responses.alias('response'), col('claim.response_id') == col('response.response_id')) \
                      .join(dim_calendar.alias('expected_cal'), col('claim.expectedresponse_date_id') == col('expected_cal.date_id')) \
                      .join(dim_calendar.alias('response_cal'), col('claim.response_date_id') == col('response_cal.date_id')) \
                      .where(
        (col('response.response') == 'Approved') &
        (col('response_cal.date') <= col('expected_cal.date'))
    ).count() / float(fact_reimbursement_claim.count())

    print('ODSETEK_REFUND: {}'.format(question_2a))

    print('2.b) Odsetek wnioskow o rejestracje pozytywnych rozpatrzonych w terminie')
    # question_2b = spark.sql("""
    #     select
    #       (SELECT count(claimnumber) as pozytywne
    #       FROM registration_claim
    #       INNER JOIN response on registration_claim.response_id = response.response_id
    #       INNER JOIN calendar expected_cal on expectedresponse_date_id = expected_cal.date_id
    #       INNER JOIN calendar response_cal on response_date_id = response_cal.date_id
    #       where response.response = 'Approved'
    #       and response_cal.date <= expected_cal.date)
    #     /
    #     CAST(count(claimnumber) as FLOAT) AS ODSETEK_REFUND
    #     FROM registration_claim
    #     """)
    # question_2b.show()
    question_2b = fact_registration_claim.alias('claim') \
                      .join(dim_responses.alias('response'), col('claim.response_id') == col('response.response_id')) \
                      .join(dim_calendar.alias('expected_cal'), col('claim.expectedresponse_date_id') == col('expected_cal.date_id')) \
                      .join(dim_calendar.alias('response_cal'), col('claim.response_date_id') == col('response_cal.date_id')) \
                      .where(
        (col('response.response') == 'Approved') &
        (col('response_cal.date') <= col('expected_cal.date'))
    ).count() / float(fact_reimbursement_claim.count())

    print('ODSETEK_REFUND: {}'.format(question_2b))

    print('3) Najdrozszy lek w Skandynawii')
    # question_3 = spark.sql("""
    #         SELECT DISTINCT product.product_bk, product.product_name
    #           FROM reimbursement_claim
    #           INNER JOIN response on reimbursement_claim.response_id = response.response_id
    #           INNER JOIN country on reimbursement_claim.country_id = country.country_id
    #           INNER JOIN product on reimbursement_claim.product_id = product.product_id
    #           INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
    #         where
    #         reimbursement_claim.drug_price = (
    #         select
    #          MAX(reimbursement_claim.drug_price)
    #           FROM reimbursement_claim
    #           INNER JOIN response on reimbursement_claim.response_id = response.response_id
    #           INNER JOIN country on reimbursement_claim.country_id = country.country_id
    #           INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
    #           WHERE response.response = 'Approved'
    #           AND country.country_bk in ('DK', 'SE', 'NO', 'FI')
    #           AND calendar.year = 2011
    #           AND calendar.quarter = 'Q4'
    #           )
    #           AND response.response = 'Approved'
    #           AND country.country_bk in ('DK', 'SE', 'NO', 'FI')
    #           AND calendar.year = 2011
    #           AND calendar.quarter = 'Q4'
    #     """)

    max_price = fact_reimbursement_claim.alias('claim') \
               .join(dim_responses.alias('response'), col('claim.response_id') == col('response.response_id')) \
               .join(dim_countries.alias('country'), col('claim.country_id') == col('country.country_id')) \
               .join(dim_calendar.alias('calendar'), col('claim.response_date_id') == col('calendar.date_id')) \
               .where(
                    (col('response.response') == 'Approved') &
                    (col('country.country_bk').isin({'DK', 'SE', 'NO', 'FI'})) &
                    (col('calendar.year') == 2011) &
                    (col('calendar.quarter') == 'Q4')
                ).groupBy().max('drug_price').collect()[0]['max(drug_price)']

    question_3 = fact_reimbursement_claim.alias('claim') \
        .join(dim_responses.alias('response'), col('claim.response_id') == col('response.response_id')) \
        .join(dim_countries.alias('country'), col('claim.country_id') == col('country.country_id')) \
        .join(dim_products.alias('product'), col('claim.product_id') == col('product.product_id')) \
        .join(dim_calendar.alias('calendar'), col('claim.response_date_id') == col('calendar.date_id')) \
        .where((col('claim.drug_price') == max_price) &
               (col('response.response') == 'Approved') &
               (col('country.country_bk').isin({'DK', 'SE', 'NO', 'FI'})) &
               (col('calendar.year') == 2011) &
               (col('calendar.quarter') == 'Q4')
        ).select(col('product.product_bk'), col('product.product_name')).distinct()

    question_3.show()

    print('4.a) Dla jakiego leku przyznano najwyzsza refundacje kwotowo na rynku polskim w latach 2005-2010?')
    question_4a = spark.sql("""
    	select  product.product_bk, product_name, reimbursement_claimed_prc, drug_price
    	  FROM reimbursement_claim
    	  INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
    	  INNER JOIN country on reimbursement_claim.country_id = country.country_id
    	  INNER JOIN product on reimbursement_claim.product_id = product.product_id
    	  WHERE calendar.date >= '2005-01-01' AND calendar.date <= '2010-12-31'
    	  AND country.country_bk  = 'PL'
    	  AND reimbursement_claim.drug_price / reimbursement_claimed_prc = (
              SELECT MAX(reimbursement_claim.drug_price / reimbursement_claimed_prc)
              FROM reimbursement_claim
              INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
              INNER JOIN country on reimbursement_claim.country_id = country.country_id
              WHERE calendar.date >= '2005-01-01' AND calendar.date <= '2010-12-31'
              AND country.country_bk  = 'PL'
    	  )
        """)
    question_4a.show()

    print('4.b) Dla jakiego leku przyznano najwyzsza refundacje procentowo na rynku polskim w latach 2005-2010?')
    question_4b = spark.sql("""
        	select  product.product_bk, product_name, reimbursement_claimed_prc, drug_price
        	  FROM reimbursement_claim
        	  INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
        	  INNER JOIN country on reimbursement_claim.country_id = country.country_id
        	  INNER JOIN product on reimbursement_claim.product_id = product.product_id
        	  WHERE calendar.date >= '2005-01-01' AND calendar.date <= '2010-12-31'
        	  AND country.country_bk  = 'PL'
        	  AND reimbursement_claimed_prc = (
                  SELECT MAX(reimbursement_claimed_prc)
                  FROM reimbursement_claim
                  INNER JOIN calendar on reimbursement_claim.response_date_id = calendar.date_id
                  INNER JOIN country on reimbursement_claim.country_id = country.country_id
                  WHERE calendar.date >= '2005-01-01' AND calendar.date <= '2010-12-31'
                  AND country.country_bk  = 'PL'
        	  )
            """)
    question_4b.show()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("ETL with Spark") \
        .getOrCreate()

    load_datasources(spark)

    spark.stop()
