# src/02_macro_analysis.py
from pyspark.sql.functions import col, desc, split, explode, regexp_extract, when, sum as _sum
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# ---------------------------------------------------------
# 1. Standard Macro Aggregations
# ---------------------------------------------------------

# Top Publishers by Number of Releases
df_top_publishers = df_flat.groupBy("publisher") \
                           .count() \
                           .orderBy(desc("count"))

# Best Rated Games (Filter applied to ensure statistical relevance)
df_best_rated = df_flat.filter(col("total_reviews") >= 500) \
                       .select("name", "publisher", "positive_ratio", "total_reviews") \
                       .orderBy(desc("positive_ratio"), desc("total_reviews"))

# Game Releases per Year (Highlights Covid-19 impact)
df_releases_per_year = df_flat.filter(col("release_year").isNotNull()) \
                              .groupBy("release_year") \
                              .count() \
                              .orderBy("release_year")

# Pricing and Discount Distribution
df_discount_status = df_flat.withColumn(
    "is_discounted", 
    (col("discount_pct") > 0).cast("boolean")
).groupBy("is_discounted").count()

# Most Represented Languages (Exploding the comma-separated string)
df_languages = df_flat.withColumn("lang_array", split(col("languages"), r",\s*")) \
                      .withColumn("language", explode(col("lang_array"))) \
                      .groupBy("language") \
                      .count() \
                      .orderBy(desc("count"))

# ---------------------------------------------------------
# 2. Age Restriction Analysis
# ---------------------------------------------------------

# Extract numerical digits, handle empty strings, and impute the "180" typo
df_flat = df_flat.withColumn(
    "extracted_age", 
    regexp_extract(col("required_age"), r"(\d+)", 1)
).withColumn(
    "required_age_num",
    when(col("extracted_age") == "", None)
    .otherwise(col("extracted_age"))
    .cast(IntegerType())
).withColumn(
    "required_age_num",
    when(col("required_age_num") == 180, 18)
    .otherwise(col("required_age_num"))
).drop("extracted_age")

# Standard grouping (includes all games, mostly 0/Unrestricted)
df_age_restrictions = df_flat.groupBy("required_age_num") \
                             .count() \
                             .orderBy("required_age_num")

# Filtered DataFrame: Only games with an actual age restriction (> 0)
df_age_restricted_only = df_age_restrictions.filter(
    (col("required_age_num") > 0) & col("required_age_num").isNotNull()
)

# Cumulative DataFrame: Total games accessible by a given age
window_spec = Window.orderBy("required_age_num")
df_age_cumulative = df_age_restrictions.filter(col("required_age_num").isNotNull()) \
                                       .withColumn("cumulative_games_allowed", _sum("count").over(window_spec))

# ---------------------------------------------------------
# 3. Display Commands for Databricks Dashboard
# ---------------------------------------------------------
display(df_top_publishers.limit(10))
display(df_best_rated.limit(10))
display(df_releases_per_year)
display(df_discount_status)
display(df_languages.limit(10))
display(df_age_restricted_only)
display(df_age_cumulative)