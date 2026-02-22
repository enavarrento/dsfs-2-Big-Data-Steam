# src/03_price_analysis.py
from pyspark.sql.functions import col, when, avg, round, expr, min as _min, max as _max

# ---------------------------------------------------------
# 1. Base Filtering & Categorization
# ---------------------------------------------------------

# Ensure NULL prices are removed for accurate distributions
df_valid_prices = df_flat.filter(col("price_usd").isNotNull())

# Categorize continuous prices into discrete buckets
df_price_bins = df_valid_prices.withColumn(
    "price_category",
    when(col("price_usd") == 0, "1. Free")
    .when((col("price_usd") > 0) & (col("price_usd") < 5), "2. Under $5")
    .when((col("price_usd") >= 5) & (col("price_usd") < 10), "3. $5 to $9.99")
    .when((col("price_usd") >= 10) & (col("price_usd") < 20), "4. $10 to $19.99")
    .when((col("price_usd") >= 20) & (col("price_usd") < 40), "5. $20 to $39.99")
    .when((col("price_usd") >= 40) & (col("price_usd") < 60), "6. $40 to $59.99")
    .otherwise("7. $60 and above")
)

# Number of releases by bins of price
df_releases_by_price_bin = df_price_bins.groupBy("price_category") \
                                        .count() \
                                        .orderBy("price_category")

# ---------------------------------------------------------
# 2. Price Distribution Over Time (Boxplot Data)
# ---------------------------------------------------------

# Filter out pre-2005 data (sparse) and extreme price outliers (>$150) for readable visualizations
df_prices_per_year = df_valid_prices.filter(
    col("release_year").isNotNull() & 
    (col("release_year") >= 2005) & 
    (col("price_usd") <= 150)
).select("release_year", "price_usd")

# Option A: Calculate exact boxplot statistics at the Spark engine level
df_prices_boxplot_stats = df_prices_per_year.groupBy("release_year") \
    .agg(
        _min("price_usd").alias("min_price"),
        expr("percentile_approx(price_usd, 0.25)").alias("Q1_price"),
        expr("percentile_approx(price_usd, 0.50)").alias("median_price"),
        expr("percentile_approx(price_usd, 0.75)").alias("Q3_price"),
        _max("price_usd").alias("max_price"),
        round(avg("price_usd"), 2).alias("mean_price")
    ) \
    .orderBy("release_year")

# Option B: Random Sampling for Databricks native UI Boxplot (~9,000 rows)
df_prices_sampled = df_prices_per_year.sample(withReplacement=False, fraction=0.15, seed=42)

# ---------------------------------------------------------
# 3. Discount Analysis
# ---------------------------------------------------------

# Average Discount per Price Category
df_discount_by_price = df_price_bins.filter(col("discount_pct").isNotNull()) \
                                    .groupBy("price_category") \
                                    .agg(round(avg("discount_pct"), 2).alias("avg_discount_pct")) \
                                    .orderBy("price_category")

# ---------------------------------------------------------
# 4. Display Commands for Databricks Dashboard
# ---------------------------------------------------------

display(df_releases_by_price_bin) 
display(df_prices_boxplot_stats)  
display(df_prices_sampled)        
display(df_discount_by_price)