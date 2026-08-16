### PySpark Basics & DataFrame Coding

#### Q-1 Read a CSV file into a DataFrame, infer the schema, and display the first 10 rows ?
```bash
from pyspark.sql import sparksession
spark = spark.session.builder.appName("testing").getOrCreate()
df = read.spark.option("inferSchema","true").option("header","true").csv("s3://bucket/path/employees.csv")
df.show(10)
```

#### Q-2 Read a Parquet dataset from S3/ADLS and select only customer_id, order_id, amount, and order_date.
```bash
form pyspark.sql import sparksession
spark = spark.session.builder.appName("readParquet").getOrCreate()
df = read.spark.parquet("s3://bucket/path/employees.csv")
result_df = df.select(
    "customer_id","order_id","amount","order_date")
result_df.show()
```

#### Q-3 Create a DataFrame from a Python list of tuples with an explicit StructType schema.
```bash
from pyspark.sql import session 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = spark.session.builder.appName("CreateDataFrame").getOnCreate()

data = [
    (1, "Kaushal", 50000.0),
    (2, "Rahul", 60000.0),
    (3, "Amit", 55000.0)
]

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df = spark.createDataFrame(data,schema)
df.show()
df.printschema()
```

#### Q-4 Filter employees whose salary is greater than 100000 and department is either IT or Finance.
```bash

```

#### Q-5 Rename three DataFrame columns without using withColumnRenamed repeatedly.
```bash

```

#### Q-6 Add a derived column total_amount = quantity * unit_price.
```bash

```

#### Q-7 Convert a string date column from yyyy-MM-dd into a Spark DateType column.
```bash

```

#### Q-8 Find the number of distinct customers in an orders DataFrame.
```bash

```

#### Q-9 Find duplicate order_id records and return only the duplicate rows.
```bash

```

#### Q-10 Remove duplicate records while keeping the latest record based on updated_at.
```bash

```

#### Q-11 Handle null customer names by replacing them with 'UNKNOWN'.
```bash

```

#### Q-12 Calculate total sales, average sales, minimum sales, and maximum sales by store.
```bash

```
#### Q-13 Return the top 10 products by total revenue.
```bash

```

#### Q-14 Find all customers who have never placed an order.
```bash

```

#### Q-15 Union two monthly DataFrames when the column order is different.
```bash

```