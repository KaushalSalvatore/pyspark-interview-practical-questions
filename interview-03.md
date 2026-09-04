#### Q-1 Return only the latest reocrd for ever order drop old record ? 
```bash
order_id , customer_id , order_status , updated_at , event_id

from pyspark.sql import funcation as f
from pyspark.sql.window import window

window_spec = Window.partitionBy("order_id")\
order_by(F.col("updated_ad").desc())

result = (df.withColumn("rn", F.row_number().over("window_spec")).
filter(F.col("rn") == 1).
drop(rn)
)

result.show()
```
#### Q-2 find the total transaction amount for each product of group and total amount in decending order ? 
```bash
products 
id      name        grp
1       dsire       maruti
2       nexon       tata
3       punch       tata

transaction 
trns_id     id      amount
1           2           8
2           2           8
3           2           6
4           1           4
5           3           4

from pyspark.sql import functions f 
result = (
    product_df.join(
        transation_df , products.id == prodicts.id , "inner"   
    ).groupBy(products_df.grp)
    .agg(F.sum(transaction_df.amount).alias("total_ammount"))
).orderBy(F.col("total_amount").desc())

result.show()
```

#### Q-3 give the employee salary and make a new column with salary range ?
```bash
5-10 > A 
10-15 > B
15-20 > C
20-25 > D 

val salary_df = empDf.withColumn(
    "bracket",
     when(col("salary") > 5  && col("salary") <=10 "A").
     when(col("salary") > 10  && col("salary") <=15 "B").
     when(col("salary") > 15  && col("salary") <=20 "C").
     when(col("salary") > 20  && col("salary") <=25 "D").
     otherwise("Other")
).show()
``` 

#### Q-4 Find customers whose current order amount is greater than their previous order amount.
```bash
from pyspark.sql import functions as f
from pysaprk.sql.windows import windows 

window_fun = windows.partitionBY("customer_id").orderby("amount")
result_df = df.withColumn("pre_amount", F.lag("amount").over("window_fun")).filter(F.col("salary") > F.col("pre_salary"))
result_df.show()
```

#### Q-5 Calculate each employee's salary as a percentage of their department's total salary ? 
```bash
from pyspark.sql import functions as f
from pysaprk.sql.windows import windows 

window_fun = windows.partitionBY("employee_id").orderby("salary")

result_df = df.withColumn("dept_total_salary", sum("salary").over("window_fun"))
                .withColumn("salary_pre",(F.col("salary") / F.col("department_total_salary")) * 100)

result_df.show()
```

#### Q-6 Find the highest salary and employee count by department without groupBy ?
```bash
from pyspark.sql import functions as F
from pysaprk.sql.windows import windows 

window_fun = windows.partitionBY("employee_name").orderby("salary")
result = df.withColumn("high_salary", F.Max("salary").over("window_fun")).
        df.withColumn("emp_count"), F.Count("emp_id").over("window_fun"))
result.show();
```

#### Q-7 Assign dense_rank to products based on revenue within each category.
```bash
from pyspark.sql import funcation as f 
from pyspark.sql.windows import windows
window_fun = windows.partitionBY("category").orderby(f.col("revenue"))
result = df.withColumn("cat_revenue_rank", F.dense_rank().over("window_fun"))
result.show();
```

#### Q-8 Find the third-highest salary in each department using dense_rank() ?
```bash
from pyspark.sql import functions as F
from pyspark.sql.window import windows 
window_fun = windoews.partitionby("dept").orderby(F.col("salary").desc())
result = f.withColumn("rn", dense_rank().over("window_fun")).filter(f.col("rn") ==3))
result.show()
```

#### Q-9 Find employees whose salary is above their department average using a window average.
```bash
from pyspark.sql import functions as f
from pysaprk.sql.windows import windows 

window_fun = windows.partitionBY("dept").orderby("salary")
result_df = df.withColumn("dept_avg_salary" F.AVG("salary").over("window_fun").
filter(f.col("salary")>F.col("dept_avg_salary")))
result_df.show()
```

#### Q-10 Find the first non-null status for each customer using first/last window functions.
```bash
window_fun = windows.partitionBY("customer_id").orderby("order_date")
result = df.withColumn(
        "first_status",
        F.first("status", ignorenulls=True).over(window_spec)
    )
result.show()
```

#### Q-11 Identify consecutive status changes for each account using lag()
```bash
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_fun = windows.partitionby(account_id).orderby(status_date)

result = df.withColumn("pre_status",F.lag("status").over("window_fun")).
withColumn("status_change", F.when(F.col("status") != F.col(pre_status), "changed status").otherwise("no change"))
```

#### Q-12 Calculate month-over-month revenue growth for each product.
```bash
from pyspark.sql import functions as F
from pyspark.sql.window import Window
monthly_revenue = df.groupby("year" , "month" , "productId").agg(
    F.sum("revenue").alise("monthly_revenue"))
withdow_fun = window.partitionby(productId).orderby("year", "month")
result = df.withCoulmn("montly_product_revenue", lag("monthly_revenue").over(withdow_fun))
result.show()
```

#### Q-13 Find the longest gap between two orders for every customer using lag and datediff.
```bash
```

### Joins & Broadcast Join

#### Q-14 Join customers and orders and return customers with their total order amount.
```bash
```

#### Q-15 Perform an inner join between orders and products using product_id.
```bash
```