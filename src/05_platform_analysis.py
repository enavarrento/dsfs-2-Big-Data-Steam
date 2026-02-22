# src/05_platform_analysis.py
from pyspark.sql.functions import col, sum as _sum, round, count

# 1. Overall Platform Availability
# Cast boolean columns to integers (True becomes 1, False becomes 0) and sum them
df_platform_totals = df_flat.select(
    _sum(col("is_windows").cast("int")).alias("Windows"),
    _sum(col("is_mac").cast("int")).alias("Mac"),
    _sum(col("is_linux").cast("int")).alias("Linux")
)

# Unpivot the columns using stack() to make it compatible with Databricks Bar/Pie charts
unpivot_expr = "stack(3, 'Windows', Windows, 'Mac', Mac, 'Linux', Linux) as (platform, game_count)"
df_platform_distribution = df_platform_totals.selectExpr(unpivot_expr)

# 2. Platform Adoption by Genre
# Calculate the percentage of games within each genre that support each operating system
# Filter for genres with a significant number of games (>= 100) to ensure statistical relevance
df_genre_platforms = df_genres.groupBy("single_genre") \
    .agg(
        count("*").alias("total_games"),
        round((_sum(col("is_windows").cast("int")) / count("*")) * 100, 2).alias("windows_pct"),
        round((_sum(col("is_mac").cast("int")) / count("*")) * 100, 2).alias("mac_pct"),
        round((_sum(col("is_linux").cast("int")) / count("*")) * 100, 2).alias("linux_pct")
    ) \
    .filter(col("total_games") >= 100) \
    .orderBy(col("mac_pct").desc(), col("linux_pct").desc())

# Display commands to configure in Databricks
display(df_platform_distribution)
display(df_genre_platforms.limit(20))