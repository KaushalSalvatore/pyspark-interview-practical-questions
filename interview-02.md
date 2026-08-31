#### Q-1 Use unionByName to combine DataFrames when one DataFrame contains an additional nullable column.
```bash
pysaprk.sql import funcation as F
df = df1.unionByName(df, allowmissingColumns=True)
```

#### Q-2 Convert an array column into separate rows using explode.
```bash
schema = ['Name','Languages']
df_explode = df.withColumn(language , explode(df['Languages']))
de_explode.show()
```

#### Q-3 Split a comma-separated product list into an array and flatten it into rows.
```bash
array_data = [(1, "A,B,C"), (2, "D,E"), (3, "F")]
df = spark.createDataFrame(data , ["id", "product"])
flat_df = df.withColumn("product_array", split(col("product"),",").withColumn("product",explode(col("product_array")))
final_df = flat_df.drop("product_array")
final_df.show()
```

#### Q-4 Use posexplode to return both the array position and element.
```bash
data = [(1, ["apple", "banana", "cherry"]), (2, ["date", "elderberry"])]
df = spark.createDataFrame(data, ["id", "fruits"])
result_df = df.select("id",poseexplode("fruits").alias("pos","val"))
result.show()
```

#### Q-5 Extract year, month, day, and day-of-week from order_date.
```bash
```

### PySpark Window Functions

#### Q-6 Find the second-highest salary in each department using row_number().
```bash
```

#### Q-7 Find the top 3 highest-paid employees in each department.
```bash
```

#### Q-8 Find the latest order for every customer using row_number().
```bash
```

#### Q-9 Find the first order and last order date for every customer using window functions.
```bash
```

#### Q-10 Calculate a running total of sales by customer ordered by order_date.
```bash
```

#### Q-11 Calculate a cumulative monthly sales total for each store.
```bash
```

#### Q-12 Calculate a 3-day moving average of sales using rowsBetween.
```bash
```

#### Q-13 Calculate the previous order amount for each customer using lag().
```bash
```

#### Q-14 Calculate the next order amount for each customer using lead().
```bash
```

#### Q-15 Find the number of days between consecutive orders for each customer.
```bash
```