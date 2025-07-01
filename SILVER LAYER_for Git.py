# Databricks Notebook - Bronze to Silver Layer ETL
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------------
# 🔐 Set up Azure Data Lake credentials
# -------------------------------------
service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# -------------------------------------
# 📁 Define base paths
# -------------------------------------
bronze_path = "abfss://bronze30062025@awsdatalake30062025.dfs.core.windows.net/"
silver_path = "abfss://silver30062025@awsdatalake30062025.dfs.core.windows.net/"

# -------------------------------------
# 📅 Load & transform: Calendar
# -------------------------------------
df_cal = (
    spark.read.format("csv").option("header", True).option("inferSchema", True)
    .load(f"{bronze_path}AdventureWorks_Calendar/")
    .withColumn("Month", month(col("Date")))
    .withColumn("Year", year(col("Date")))
)
df_cal.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Calendar").save()

# -------------------------------------
# 👤 Load & transform: Customers
# -------------------------------------
df_cus = (
    spark.read.format("csv").option("header", True).option("inferSchema", True)
    .load(f"{bronze_path}AdventureWorks_Customers/")
    .withColumn("fullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))
)
df_cus.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Customers").save()

# -------------------------------------
# 📁 Load & write: Product Categories
# -------------------------------------
df_procat = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{bronze_path}AdventureWorks_Product_Categories/")
df_procat.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Product_Categories").save()

# -------------------------------------
# 📦 Load & transform: Products
# -------------------------------------
df_pro = (
    spark.read.format("csv").option("header", True).option("inferSchema", True)
    .load(f"{bronze_path}AdventureWorks_Products/")
    .withColumn("ProductSKU", split(col("ProductSKU"), "-")[0])
    .withColumn("ProductName", split(col("ProductName"), " ")[0])
)
df_pro.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Products").save()

# -------------------------------------
# 🔁 Load & write: Returns
# -------------------------------------
df_ret = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{bronze_path}AdventureWorks_Returns/")
df_ret.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Returns").save()

# -------------------------------------
# 🌍 Load & write: Territories
# -------------------------------------
df_ter = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{bronze_path}AdventureWorks_Territories/")
df_ter.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Territories").save()

# -------------------------------------
# 🧩 Load & write: Subcategories
# -------------------------------------
df_subcat = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{bronze_path}Product_Subcategories/")
df_subcat.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_SubCategories").save()

# -------------------------------------
# 💵 Load & transform: Sales
# -------------------------------------
df_sales = (
    spark.read.format("csv").option("header", True).option("inferSchema", True)
    .load(f"{bronze_path}AdventureWorks_Sales*")
    .withColumn("StockDate", to_timestamp("StockDate"))
    .withColumn("OrderNumber", regexp_replace(col("OrderNumber"), "S", "T"))
    .withColumn("multiply", col("OrderLineItem") * col("OrderQuantity"))
)
df_sales.write.format("parquet").mode("append").option("path", f"{silver_path}AdventureWorks_Sales").save()

# -------------------------------------
# 📊 Quick Insight (optional): Orders per day
# -------------------------------------
df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("total_order")).display()
