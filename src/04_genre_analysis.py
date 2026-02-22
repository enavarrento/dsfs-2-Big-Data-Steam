# src/04_genre_analysis.py
from pyspark.sql.functions import col, count, avg, desc, regexp_replace, regexp_extract, round, sum as _sum
from pyspark.sql.types import DoubleType

# 1. Most represented genres
df_genre_counts = df_genres.groupBy("single_genre") \
                           .agg(count("*").alias("game_count")) \
                           .orderBy(desc("game_count"))

# 2. Genres with the best positive/negative review ratio
# Filter for games with >= 500 reviews, and ensure the genre has at least 10 qualified games to avoid outliers
df_genre_ratings = df_genres.filter(col("total_reviews") >= 500) \
                            .groupBy("single_genre") \
                            .agg(
                                round(avg("positive_ratio"), 2).alias("avg_positive_ratio"),
                                count("*").alias("qualified_games")
                            ) \
                            .filter(col("qualified_games") >= 10) \
                            .orderBy(desc("avg_positive_ratio"))

# 3. Publisher's favorite genres
# To keep the visualization readable, filter for a subset of top publishers first
# Note: df_top_publishers was created in the macro_analysis.py script
top_publishers = [row['publisher'] for row in df_top_publishers.limit(5).collect()]

df_publisher_genres = df_genres.filter(col("publisher").isin(top_publishers)) \
                               .groupBy("publisher", "single_genre") \
                               .count() \
                               .orderBy("publisher", desc("count"))

# 4. Most lucrative genres (Proxy Calculation)
# Extract the lower bound of the 'owners' range, remove commas, and cast to numeric
df_lucrative = df_genres.withColumn(
    "min_owners", 
    regexp_replace(regexp_extract(col("owners"), r"^([\d,]+)", 1), ",", "").cast(DoubleType())
).withColumn(
    "estimated_revenue_usd", 
    col("min_owners") * col("price_usd")
)

df_genre_revenue = df_lucrative.filter(col("estimated_revenue_usd").isNotNull()) \
                               .groupBy("single_genre") \
                               .agg(
                                   round(_sum("estimated_revenue_usd"), 2).alias("total_estimated_revenue"),
                                   round(avg("estimated_revenue_usd"), 2).alias("avg_estimated_revenue_per_game")
                               ) \
                               .orderBy(desc("total_estimated_revenue"))

# Display commands to configure in Databricks
display(df_genre_counts.limit(20))
display(df_genre_ratings.limit(20))
display(df_publisher_genres)
display(df_genre_revenue.limit(20))