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
from pyspark.sql import functions f 

result_df = (
    df.withCoulmn("year", F.year("order_date")).
    ("month", F.month("order_date")).
    ("day", F.col("order_date")).
    ("day_of_week", F.col("order_date")).
    )
```

### PySpark Window Functions

#### Q-6 Find the second-highest salary in each department using row_number().
```bash
pyspark.sql import function as f 
pyspark.sql.windows import windows 

window_fun = Windows.partitionBy("dept").orderBy(F.col("salary").desc())
result_df = (df.withColumn("rn" , F.ROW_NUMBER().OVER("window_fun")).filter(F.col(rn == 2)))
result_df.show()
```

#### Q-7 Find the top 3 highest-paid employees in each department.
```bash
pyspark.sql import fucnation as F
pyspark.sql.windows impoert windows
windows_fun = windows.partitionBy("dept").orderBy(F.col("salary").desc())
result = (df.withColumn("rn" , F.dence_rank().over("window_fun")).filterBy(F.col("rn" < = 3 )))
result_df.show()
```

#### Q-8 Find the latest order for every customer using row_number().
```bash
pyspark.sql import funcation as F
pyspark.sql.window import windows 
win_fun = windows.partitionBy("customer_id").orderBy(f.col("order_date").desc())
result_df = (df.withColumn("rn", F.row_number().over("win_fun").filterBy(f.col(rn==1).drop("rn"))))
result_df.show()
```

#### Q-9 Find the first order and last order date for every customer using window functions.
```bash
first() and last() as window functions.

pyspark.sql import funcation as F
pyspark.sql.window import windows 

window_fun = windows.partitionBy("customer_id").orderBy(f.col("order_date").desc().rowBetween(
    Window.unboundedpreceding,
    Window.unboundfollowing
)
)

result.df = df.withColumn("first_order", f.first("order_date").over("window_fun").
            .withColumn("last_order_date",F.last("order_date").over(window_spec)
    )
)

result_df.select( "customer_id",
    "first_order_date",
    "last_order_date").distict().show()
```

#### Q-10 Calculate a running total of sales by customer ordered by order_date.
```bash
from pyspark.sql import functions f
window_fun = windows.partitionBy("customer_id").orderBy(f.col("order_date").desc().rowBetween(
    Window.unboundedpreceding,Window.unboundfollowing))
resumt = df.withColumn("total_sale", F.sum("amount").over("window_fun"))
result.show()
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