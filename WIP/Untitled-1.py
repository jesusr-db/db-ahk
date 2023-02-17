
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
  StructField('CustomerID', IntegerType(), False),
  StructField('FirstName',  StringType(),  False),
  StructField('LastName',   StringType(),  False)
])

data = [
  [ 1000, 'Mathijs', 'Oosterhout-Rijntjes' ],
  [ 1001, 'Joost',   'van Brunswijk' ],
  [ 1002, 'Stan',    'Bokenkamp' ]
]

customers = spark.createDataFrame(data, schema)
customers.show()
