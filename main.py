# -*- coding: utf-8 -*-
"""
Created on Thu Dec 03 01:17:08 2020

@author: Jagadeesh 
"""

import logging
from  functSpark import *

from datetime import datetime

logging.basicConfig(filename=datetime.now().strftime('/sparklogs/spark_%H:%M_%Y_%m_%d.log'),level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')


applName = "IBM Assignment"
mastConf = "local[4]"

applName = "IBM Assignment"
inst = ibmAssignment(applName,mastConf)

logging.info("Spark Session has been Created!")

inst.sourceDataIngestion(filetype="csv",location="cos://candidate-exercise.myCos/emp-data.csv")

logging.info("Source Data retrived")

inst.dataPreprocessing()

logging.info("Data processing stage completed")

#4.	Develop Scala code to:
#a.	Create a table based COS data schema read in Step 2
#b.	Write the contents read from the COS object in Step 2 to into this table 

inst.mysqlTableLoad(tableName=datetime.now().strftime('Employee%Y%m%d'))

logging.info("Table create with source file schema")

#5.	Develop Scala code to read the same data from the database and display frist 20 lines.  Also develop code to calculate and display the following:

inst.tableDataViewer(tableName=datetime.now().strftime('Employee%Y%m%d'))

logging.info("Data retrived from Database")

#a.	Gender ratio in each department
#b.	Average salary in each department
#c.	Male and female salary gap in each department

inst.aggregator(tempTableName=datetime.now().strftime('Employee_Temp_%Y%m%d'))

logging.info("""
a.	Gender ratio in each department
b.	Average salary in each department
c.	Male and female salary gap in each department
Calculation completed.
""")

#Develop Scala code to write at least one of the data calculated in step 5 as multi part Parquet files as COS object in the bucket information provided in Step 1
#cos://candidate-exercise.myCos/candidate-exercise

inst.cosBucketWriter(bucketName="candidate-exercise",fileFormat="parquet")

inst.cosBucketVerifier(bucketName="candidate-exercise",fileFormat="parquet")

logging.info("Spark Session Ended")