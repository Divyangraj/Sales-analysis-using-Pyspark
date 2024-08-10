# Databricks notebook source
/FileStore/tables/menu_csv.txt
/FileStore/tables/sales_csv.txt


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema=StructType([
    StructField("Product_Id",IntegerType(),True),
    StructField("Customer_Id",StringType(),True),
    StructField("Order_Date",DateType(),True),
    StructField("Location",StringType(),True),
    StructField("Source_Order",StringType(),True)
])

sales_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt")

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Adding Year,Month,Quarter
from pyspark.sql.functions import month,year,quarter

# COMMAND ----------

sales_df = sales_df.withColumn("Order_Year",year(sales_df.Order_Date))
sales_df = sales_df.withColumn("Order_Month",month(sales_df.Order_Date))
sales_df = sales_df.withColumn("Order_Quarter",quarter(sales_df.Order_Date))
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Menu DataFrame
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema=StructType([
    StructField("Product_Id",IntegerType(),True),
    StructField("Product_Name",StringType(),True),
    StructField("Price",StringType(),True),
])

menu_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")

display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each customer
total_amouny_spent = (sales_df.join(menu_df,'Product_Id').groupBy('Customer_Id').agg({'Price':'sum'}).orderBy('Customer_Id'))

display(total_amouny_spent)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each food category
total_amouny_spent_category = (sales_df.join(menu_df,'Product_Id').groupBy('Product_Name').agg({'Price':'sum'}).orderBy('Product_Name'))

display(total_amouny_spent_category)

# COMMAND ----------

# DBTITLE 1,Total amount of sale in each month
monthly_sale = (sales_df.join(menu_df,'Product_Id').groupBy('Order_Month').agg({'Price':'sum'}).orderBy('Order_Month'))

display(monthly_sale)

# COMMAND ----------

# DBTITLE 1,Yearly sale
Yearly_sale = (sales_df.join(menu_df,'Product_Id').groupBy('Order_Year').agg({'Price':'sum'}).orderBy('Order_Year'))

display(Yearly_sale)

# COMMAND ----------

# DBTITLE 1,Quarterly sale
Quarterly_sale = (sales_df.join(menu_df,'Product_Id').groupBy('Order_Quarter').agg({'Price':'sum'}).orderBy('Order_Quarter'))

display(Quarterly_sale)

# COMMAND ----------

# DBTITLE 1,How many time each product purchased
from pyspark.sql.functions import count

most_df = (sales_df.join(menu_df,'Product_Id').groupBy("Product_Id","Product_Name")
           .agg(count("Product_Id").alias("Product_Count")).orderBy("Product_Count",ascending=0).drop("Product_id"))

display(most_df)           

# COMMAND ----------

# DBTITLE 1,Top ordered item
top_order = (sales_df.join(menu_df,'Product_Id').groupBy("Product_Id","Product_Name")
           .agg(count("Product_Id").alias("Product_Count")).orderBy("Product_Count",ascending=0).drop("Product_id").limit (1))
         

display(top_order)

# COMMAND ----------

# DBTITLE 1,frequency of customer visited
from pyspark.sql.functions import countDistinct

df = (sales_df.filter(sales_df.Source_Order=="Restaurant").groupBy("Customer_Id")
      .agg(countDistinct("Order_Date")))
display(df)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
sales_by_country = (sales_df.join(menu_df,"Product_Id").groupBy("Location")
                    .agg({"Price":"sum"}))

display(sales_by_country)


# COMMAND ----------

# DBTITLE 1,Total sales by order source
orders_by_source = (sales_df.join(menu_df,"Product_Id").groupBy("Source_Order")
                    .agg({"Price":"sum"}))

display(orders_by_source)

# COMMAND ----------


