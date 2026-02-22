# src/01_data_preparation.py (to be run in the Databricks notebook)
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, split, explode, to_date, year, round

# 1. Flatten the main structure
df_flat = df_raw.select("id", "data.*")

# 2. Extract platforms into distinct boolean columns
df_flat = df_flat.withColumn("is_windows", col("platforms.windows")) \
                 .withColumn("is_mac", col("platforms.mac")) \
                 .withColumn("is_linux", col("platforms.linux"))

# 3. Clean dates and extract release year
# Format "yyyy/M/d" handles both single and double-digit months/days
df_flat = df_flat.withColumn("parsed_release_date", to_date(col("release_date"), "yyyy/M/d")) \
                 .withColumn("release_year", year(col("parsed_release_date")))

# 4. Clean prices and cast to numeric
# Dividing by 100 under the assumption that "999" translates to $9.99
df_flat = df_flat.withColumn("price_usd", col("price").cast(DoubleType()) / 100) \
                 .withColumn("discount_pct", col("discount").cast(DoubleType()))

# 5. Feature Engineering: Total reviews and positive review ratio
df_flat = df_flat.withColumn("total_reviews", col("positive") + col("negative")) \
                 .withColumn("positive_ratio", round(col("positive") / col("total_reviews") * 100, 2))

# 6. Prepare an exploded DataFrame specifically for genre analysis
# Split the comma-separated genre string and explode it into multiple rows
df_genres = df_flat.withColumn("genre_array", split(col("genre"), ",\s*")) \
                   .withColumn("single_genre", explode(col("genre_array")))

# Display results for verification
display(df_flat.select("name", "release_year", "price_usd", "is_windows", "total_reviews", "positive_ratio").limit(10))