import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, lit
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer
from time import mktime, strptime


CSV_LOCATION = "OnlineRetail.csv"

dateToTsUdf = udf(lambda date: int(mktime(strptime(date,"%m/%d/%Y %H:%M"))) if date is not None else None)

invoiceSchema = StructType([
  StructField('InvoiceNo', StringType(), False), 
  StructField('StockCode', StringType(), False),
  StructField('Description', StringType(), True),
  StructField('Quantity', IntegerType(), False),
  StructField('InvoiceDate', StringType(), False),
  StructField('UnitPrice', DoubleType(), False),
  StructField('CustomerID', IntegerType(), True),
  StructField('Country', StringType(), False)
])

invoicesDf = spark.read.options(header=True).schema(invoiceSchema).csv(CSV_LOCATION)
# invoicesDf = spark.read.format('csv').options(header=True, inferSchema=True).load(CSV_LOCATION)
invoicesDf = invoicesDf.withColumn("InvoiceDate", dateToTsUdf(invoicesDf.InvoiceDate).cast(IntegerType()))

invoicesDf.show(2)
invoicesDf.printSchema()

invoicesDf.registerTempTable("invoices")
invoicesCleanDf = sqlContext.sql(
  "SELECT * \
  FROM invoices \
  WHERE Description IS NOT NULL \
  AND Description != 'Manual' \
  AND Description != 'POSTAGE' \
  AND Description != 'Discount' \
  AND Description != 'DOTCOM POSTAGE' \
  AND Description != 'AMAZON FEE' \
  AND Quantity > 0 \
  AND UnitPrice > 0"
)

invoicesCleanDf.count()

productIndexer = StringIndexer(inputCol="Description", outputCol="ProductIndex").fit(invoicesCleanDf)
timeSeriesDF = productIndexer.transform(invoicesCleanDf).select(["InvoiceDate", "InvoiceNo", "CustomerID", "Description", "ProductIndex", "Quantity"])
timeSeriesDF.show(3)

timeSeriesDF.select("CustomerID").distinct().count()
timeSeriesDF.select("ProductIndex").distinct().count()

