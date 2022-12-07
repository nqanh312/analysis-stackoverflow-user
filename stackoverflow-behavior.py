from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/Asm1Dep303') \
        .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/Asm1Dep303') \
        .config("spark.driver.memory", "6g")\
        .enableHiveSupport()\
        .getOrCreate()

    dfQ = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("uri", "mongodb://localhost:27017/Asm1Dep303.Questions")\
        .load()

    dfA = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://localhost:27017/Asm1Dep303.Answers") \
        .load()


    dfQ1 = dfQ.drop("_id")\
        .withColumn("ClosedDate",
                    to_date(
                        when(col("ClosedDate") == "NA", None)
                        .otherwise(substring_index("ClosedDate", "T",1)),
                        "yyyy-MM-dd"))\
        .withColumn("OwnerUserId", expr('case OwnerUserId when "NA" then null else OwnerUserId end').cast(IntegerType()))\
        .withColumn("CreationDate", to_date(substring_index("CreationDate", "T",1), "yyyy-MM-dd"))\
        .withColumn("Id", dfQ.Id.cast(IntegerType())) \
        .withColumn("Score", dfQ.Score.cast(IntegerType()))

    dfA1 = dfA.drop("_id")\
        .withColumn("OwnerUserId", expr('case OwnerUserId when "NA" then null else OwnerUserId end').cast(IntegerType()))\
        .withColumn("ParentId", dfA.ParentId.cast(IntegerType()))\
        .withColumn("CreationDate", to_date(substring_index("CreationDate", "T",1), "yyyy-MM-dd"))\
        .withColumn("Id", dfA.Id.cast(IntegerType())) \
        .withColumn("Score", dfA.Score.cast(IntegerType()))

    dfQProgramLanguage = dfQ1.groupBy(when(dfQ1.Body.rlike("C#"), "C#")
                                      .when(dfQ1.Body.rlike("C\+\+"), "C++")
                                      .when(dfQ1.Body.rlike("CSS"), "CSS")
                                      .when(dfQ1.Body.rlike("HTML"), "HTML")
                                      .when(dfQ1.Body.rlike("PHP"), "PHP")
                                      .when(dfQ1.Body.rlike("SQL"), "SQL")
                                      .when(dfQ1.Body.rlike("Go"), "Go")
                                      .when(dfQ1.Body.rlike("Ruby"), "Ruby")
                                      .when(dfQ1.Body.rlike("Python"), "Python")
                                      .when(dfQ1.Body.rlike("Java"), "Java")
                                      .otherwise("Other")
                                      ).count()

    print('Yeu cau 1:')
    dfQProgramLanguageFinal = dfQProgramLanguage.withColumnRenamed(dfQProgramLanguage.columns[0], "Programming Language").show()
    
    pattern = r'href=\"http://([\w\.]+)/'
    dfQdomain = dfQ1.withColumn("domain", regexp_extract(dfQ1.Body, pattern, 1))\
                    .select("domain")\
                    .filter(col("domain") != "")

    print('Yeu cau 2:')
    dfQdomainFinal = dfQdomain.groupBy("domain").count().orderBy(col("count").desc()).show()

    windowScore = Window.partitionBy(dfQ1.OwnerUserId).orderBy(dfQ1.CreationDate)
    print('Yeu cau 3:')
    dfQsccore = dfQ1.select("OwnerUserId", "CreationDate", sum("Score").over(windowScore).alias("TotalScore"))\
        .orderBy("OwnerUserId", col("CreationDate").asc())\
        .filter(dfQ1.OwnerUserId.isNotNull())\
        .show()


    startDate = "2008-01-01"
    endDate = "2009-01-01"

    print('Yeu cau 4:')
    dfQScore1 = dfQ1.filter(dfQ1.CreationDate.between(startDate,endDate) & dfQ1.OwnerUserId.isNotNull())\
        .groupBy(dfQ1.OwnerUserId)\
        .agg(sum(dfQ1.Score)).alias("TotalScore")\
        .orderBy(dfQ1.OwnerUserId)\
        .show()

    #spark.sql("CREATE DATABASE IF NOT EXISTS asm1_db")
    spark.sql("USE asm1_db")

    dfQ1.write.bucketBy(2, "Id").mode("overwrite").saveAsTable("asm1_db.tableQ1")
    dfA1.write.bucketBy(2, "ParentId").mode("overwrite").saveAsTable("asm1_db.tableA1")

    statement = '''
    select q.Id, count(*) as count
    from tableq1 as q join tablea1 as a on q.Id = a.ParentId
    group by q.Id
    having count(*) > 5
    order by q.Id
    '''
    dfMoreThan5 = spark.sql(statement)
    print('Yeu cau 5:')
    dfMoreThan5.show()

    statement2 = '''
        select q.OwnerUserId
        from tableq1 as q join tablea1 as a on q.Id = a.ParentId and  a.CreationDate = q.CreationDate
        where q.OwnerUserId is not null
        group by q.Id, q.OwnerUserId
        having count(*) > 5
        '''
    cond2 = spark.sql(statement2)

    statement1 = '''
    select OwnerUserId
    from tablea1
    where OwnerUserId is not null
    group by OwnerUserId
    having count(*) > 50 or sum(Score) > 500
    '''

    cond1 = spark.sql(statement1)


    condAll = cond2.union(cond1).distinct().orderBy("OwnerUserId")
    print('Yeu cau 6:')
    condAll.show()
