### Joins & Broadcast Join

#### Q-1 Join customers and orders and return customers with their total order amount.
```bash
from pyspark.sql import functions as F

resultDf = customers.join(orders , on= "order_id", how="inner").filter(f.col("status") == "return")
resultDf.show()

-- Another safer way 

result_df = (customers.alias("c").join(orders.alias("o"),F.col("c.order_id") == F.col("o.order_id"),"inner")
    .filter(F.col("o.status") == "return")
    .select("c.*", "o.order_id", "o.status"))
result_df.show()
```

#### Q-2 Perform an inner join between orders and products using product_id.
```bash
result = orders.join("prodicts",on="prodcutID",how="inner"); 
```

#### Q-3 Perform a left anti join to find customers with no matching orders.
```bash
It returns only the rows from the left DataFrame that have NO matching row in the right DataFrame.

from pyspark.sql import funcations as F
from pyspark.sql.window import window
df = df.customer.join(store , on = "customerId", how='left anti')
df.show()
```

#### Q-4 Perform a left semi join to return customers who have at least one order
```bash
Left Semi Join = Give me rows from the LEFT DataFrame that have a matching row in the RIGHT DataFrame.

from pyspark.sql import funcation as F 
from pyspark.sql.window import window 
df = df.customer.join(order , on ="orderID" , how="left_semi")
df.show()
```

#### Q-5 Join employee and department tables and handle duplicate column names safely.
```bash
import pyspark.sql import funcations as f 
import pyspark.sql.window import window 

result_df = (
    employee.alise("e").join(
        department.alise("d"),
        f.col("e.empID") = f.col("d.empID"),
        "left"
    ).
    groupBY( F.col("d.deptID")).
    agg.(f.count("e.empID").alise("employee_count")).
    filter(f.col("employee_count") > 2);
)
``` 

#### Q-6 Join three DataFrames: customers, orders, and payments.
```bash

```

#### Q-7 Use a broadcast join to join a large orders DataFrame with a small country lookup DataFrame
```bash
```

#### Q-8 Write code to force a broadcast join and explain how to verify it in the physical plan.
```bash
```

#### Q-9 Compare a normal join and broadcast join using explain('formatted').
```bash
```

#### Q-10 Broadcast a small dimension table and calculate sales by region.
```bash
```

#### Q-11 Handle a situation where the lookup table unexpectedly becomes too large for broadcast
```bash
```

#### Q-12 Join two large DataFrames on customer_id and explain how you would reduce shuffle.
```bash
```

#### Q-13 Perform a range join between transactions and effective-dated customer records.
```bash
```

#### Q-14 Join fact data to an SCD Type 2 dimension using business_key and effective date range.
```bash
```

#### Q-15 Find records present in source but missing in target using a left anti join
```bash
```

