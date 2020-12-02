# -*- coding: utf-8 -*-
"""
Created on Wed Dec 02 01:17:08 2020

@author: Jagadeesh 
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime
import logging

logging.basicConfig(filename=datetime.now().strftime('/sparklogs/spark_%H:%M_%Y_%m_%d.log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')


#appName("IBM Assignment")
#master("local[4]")

class ibmAssignment:

    def __init__(self,applName,mastConf):
        '''
        This method would initialize/creates spark session.
        '''             
        spark = SparkSession.builder.appName(applName).master(mastConf).getOrCreate()
        logging.info(spark)
        logging.info("Spark Session created")

        """ sourceDF = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("cos://candidate-exercise.myCos/emp-data.csv") """

    def sourceDataIngestion(self,filetype,location):
        '''
        This method would created data frame out of source data!
        '''
        self.filetype = filetype

        if lower(filetype) != "jdbc":
            self.sourceDF = spark.read \
                .format(self.filetype) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(location)
            logging.info("Source file has been captured!")
        else:
            #these hardcoded values could be parameterized or picked from global config files, for solving this in timely manner i've done this.
            self.sourceDF = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://localhost/sparktest") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", "sparktest.cosdb") \
                .option("user", "root") \
                .option("useSSL","false") \
                .option("password", "redhat@123") \
                .load()
            logging.info("Source Data has been captured from Database")
        return self.sourceDF

    def dataPreprocessing(self):
        self.processedDF = self.sourceDF.select("Name","Gender","Department",f.regexp_replace("salary",'[$,]','').alias("Salary"),"Loc","Rating")
        logging.info("Data preprocessing stage completed")
        return self.processedDF

#4.	Develop Scala code to:
#a.	Create a table based COS data schema read in Step 2
#b.	Write the contents read from the COS object in Step 2 to into this table 

    def mysqlTableLoad(self,tableName):

        '''
        This function would load processed data from DF to mysql table based on your table input
        '''
        self.processedDF.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost/sparktest") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", "sparktest.{}".format(tableName)) \
            .option("user", "root") \
            .option("useSSL","false") \
            .option("password", "redhat@123") \
            .save()
        logging.info("Table has been loaded with clean data")

        print("Data has been dumped into table {} successfully".format(tableName))


#5.	Develop Scala code to read the same data from the database and display frist 20 lines.  Also develop code to calculate and display the following:

    
    def tableDataViewer(self,tableName):
        '''
        this method would allow us to read data from database and display frist 20 lines
        '''
        sourceTableDF = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost/sparktest") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", "sparktest.{}".format(tableName)) \
            .option("user", "root") \
            .option("useSSL","false") \
            .option("password", "redhat@123") \
            .load()
        logging.info("Fetching data from DataBase and showing 20 sample records")
        return sourceTableDF.show(20)

#a.	Gender ratio in each department
#b.	Average salary in each department
#c.	Male and female salary gap in each department

    def aggregator(self,tempTableName):
        '''
        This functino would perform all aggregations and transformations.
        '''
        self.processedDF.registerTempTable(tempTableName)
        logging.info("Stage table has been created")
 
        genderRatioPerDept = spark.sql(""" \
            select \
            round((sum(case when gender = 'Male' then 1 else 0 end)/count(*)) * 100,2) as male_ratio, \
            round((sum(case when gender = 'Female' then 1 else 0 end)/count(*)) * 100,2) as female_ratio, \
            department \
            from {} \
            where department <> 'NULL' \
            group by department \
            """).format(tempTableName)
        
        genderRatioPerDept.show()
        logging.info("genderRatioPerDept has been calculated")

        avgSalaryPerDept = spark.sql("""\
            select round(avg(nvl(regexp_replace(salary,'[$,]',''),0)),2)as avgSalaryPerDept, \
            department 
            from {}
            group by department \
            """).format(tempTableName)

        avgSalaryPerDept.show()

        logging.info("avgSalaryPerDept has been calculated")

        self.maleVsFemaleSalaryGap = spark.sql(""" \
            select department,ROUND(AVG(ifnull(regexp_replace(salary,'[$,]',''),0)),0) sum_job_salary, \
            round(sum(case when gender='Male' then ifnull(regexp_replace(salary,'[$,]',''),0) end),0) sum_m_salary, \
            round(sum(case when gender='Female' then ifnull(regexp_replace(salary,'[$,]',''),0) end),0) sum_f_salary, \
            abs(round(sum(case when gender='Female' then ifnull(regexp_replace(salary,'[$,]',''),0) end),0) - round(sum(case when gender='Male' then ifnull(regexp_replace(salary,'[$,]',''),0) end),0)) diff_in_sum \
            from {} \
            where department <> 'NULL' \
            group by department \
            order by department \
            """).format(tempTableName)
        
        maleVsFemaleSalaryGap.show()
        logging.info("maleVsFemaleSalaryGap has been calculated")

        return self.maleVsFemaleSalaryGap
    
    
#Develop Scala code to write at least one of the data calculated in step 5 as multi part Parquet files as COS object in the bucket information provided in Step 1
#cos://candidate-exercise.myCos/candidate-exercise
    def cosBucketWriter(self,bucketName,fileFormat):
        '''
        This method would write aggregated output to COS bucket in IBM cloud
        '''
        self.maleVsFemaleSalaryGap.write \
            .format(fileFormat) \
            .option("compression","snappy") \
            .mode("overwrite") \
            .save("cos://candidate-exercise.myCos/{}".format(bucketName))
