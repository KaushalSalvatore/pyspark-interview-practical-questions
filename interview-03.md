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


#### Q-4

- 

```bash
```

#### Q-5

- 

```bash
```

#### Q-6 

- 

```bash
```

#### Q-7 

- 

```bash
```

#### Q-8

- 

```bash
```

#### Q-9

- 

```bash
```

#### Q-10
- 

```bash
```


#### Q-11
- 

```bash
```


#### Q-12
- 

```bash
```


#### Q-13
- 

```bash
```

#### Q-14
- 

```bash
```


#### Q-15
- 

```bash
```