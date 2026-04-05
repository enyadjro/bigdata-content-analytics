// ============================================================
// Project: Streaming-Scale Content Analytics with Spark
// Script: spark_content_analytics.scala
// Purpose: Analyze large-scale IMDb-style content metadata
//          for strategic insights on genre trends, talent
//          performance, TV benchmarking, and localization
// ============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// ------------------------------------------------------------
// 1. Load raw datasets
// ------------------------------------------------------------
val basics = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/title.basics.tsv")

val ratings = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/title.ratings.tsv")

val crew = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/title.crew.tsv")

val names = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/name.basics.tsv")

val episodes = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/title.episode.tsv")

val akas = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .option("nullValue", "\\N")
  .load("hdfs:/user/maria_dev/final/movie/title.akas.tsv")

// ------------------------------------------------------------
// 2. Clean core datasets
// ------------------------------------------------------------
val basicsClean = basics
  .filter($"tconst".isNotNull)

val ratingsClean = ratings
  .filter($"averageRating".isNotNull && $"numVotes".isNotNull)
  .withColumn("averageRating", $"averageRating".cast("double"))
  .withColumn("numVotes", $"numVotes".cast("int"))

val crewClean = crew
  .filter($"directors".isNotNull)
  .withColumn("director", explode(split($"directors", ",")))
  .select("tconst", "director")

val namesClean = names
  .filter($"primaryName".isNotNull)
  .withColumn("birthYear", when($"birthYear".isNull, null).otherwise($"birthYear".cast("int")))
  .withColumn("deathYear", when($"deathYear".isNull, null).otherwise($"deathYear".cast("int")))
  .select("nconst", "primaryName", "birthYear", "deathYear")

// ------------------------------------------------------------
// 3. Analysis A: Genre trend detection over rolling decades
//    Business use case: content investment strategy
// ------------------------------------------------------------
val movieGenres = basicsClean
  .filter($"titleType" === "movie")
  .filter($"startYear".isNotNull && $"genres".isNotNull)
  .withColumn("startYear", $"startYear".cast("int"))
  .withColumn("genre", explode(split($"genres", ",")))
  .join(ratingsClean, "tconst")
  .filter($"numVotes" >= 1000)

val genreCounts = movieGenres.groupBy("genre").agg(count("*").as("genreCount"))
val eligibleGenres = genreCounts.filter($"genreCount" >= 500)

val filteredGenres = movieGenres.join(eligibleGenres, "genre")

val genreYearAgg = filteredGenres.groupBy("genre", "startYear")
  .agg(
    avg($"averageRating").as("yearAvgRating"),
    count("*").as("numMovies"),
    avg($"numVotes").as("avgVotes")
  )

val currentWindow = Window.partitionBy("genre").orderBy("startYear").rowsBetween(-9, 0)
val previousWindow = Window.partitionBy("genre").orderBy("startYear").rowsBetween(-10, -1)

val rollingGenreStats = genreYearAgg
  .withColumn("currentRollingAvg", avg($"yearAvgRating").over(currentWindow))
  .withColumn("previousRollingAvg", avg($"yearAvgRating").over(previousWindow))
  .withColumn("ratingIncrease", $"currentRollingAvg" - $"previousRollingAvg")

rollingGenreStats.createOrReplaceTempView("rollingGenreStats")

val topGenreTrend = spark.sql("""
WITH bestIncrease AS (
    SELECT genre, startYear, ratingIncrease,
           ROW_NUMBER() OVER (PARTITION BY genre ORDER BY ratingIncrease DESC) AS rn
    FROM rollingGenreStats
    WHERE previousRollingAvg IS NOT NULL
),
bestGenreIncrease AS (
    SELECT genre, startYear, ratingIncrease
    FROM bestIncrease
    WHERE rn = 1
),
allTimeAvg AS (
    SELECT genre, ROUND(AVG(yearAvgRating), 2) AS allTimeAvgRating
    FROM rollingGenreStats
    GROUP BY genre
)
SELECT b.genre,
       a.allTimeAvgRating,
       CONCAT(b.startYear, '-', b.startYear + 9) AS bestPeriod,
       ROUND(b.ratingIncrease, 2) AS bestIncrease
FROM bestGenreIncrease b
JOIN allTimeAvg a ON b.genre = a.genre
ORDER BY bestIncrease DESC
LIMIT 3
""")

println("=== Top Genre Trend Opportunities ===")
topGenreTrend.show(false)

// ------------------------------------------------------------
// 4. Analysis B: Top director performance
//    Business use case: talent investment strategy
// ------------------------------------------------------------
val directorMovies = basicsClean
  .filter($"titleType" === "movie")
  .filter($"startYear".isNotNull && $"genres".isNotNull)
  .withColumn("startYear", $"startYear".cast("int"))
  .join(ratingsClean, "tconst")
  .join(crewClean, "tconst")
  .join(namesClean, $"director" === $"nconst")
  .withColumn("directorAge", $"startYear" - $"birthYear")
  .filter($"numVotes" >= 1000)
  .filter($"directorAge" > 30)
  .filter($"deathYear".isNull || $"startYear" <= $"deathYear")

directorMovies.createOrReplaceTempView("directorMovies")

val topDirectors = spark.sql("""
WITH directorStats AS (
    SELECT primaryName, birthYear, deathYear,
           COUNT(*) AS filmCount,
           ROUND(AVG(averageRating), 2) AS avgRating
    FROM directorMovies
    GROUP BY primaryName, birthYear, deathYear
    HAVING COUNT(*) >= 10
),
genreCounts AS (
    SELECT primaryName, EXPLODE(SPLIT(genres, ',')) AS genre
    FROM directorMovies
),
genreGrouped AS (
    SELECT primaryName, genre, COUNT(*) AS genreCount
    FROM genreCounts
    GROUP BY primaryName, genre
),
topGenres AS (
    SELECT primaryName, genre
    FROM (
        SELECT primaryName, genre,
               ROW_NUMBER() OVER (PARTITION BY primaryName ORDER BY genreCount DESC) AS rn
        FROM genreGrouped
    ) g
    WHERE rn = 1
),
finalResult AS (
    SELECT s.primaryName, s.avgRating, s.filmCount,
           g.genre AS mostPopularGenre,
           CASE
             WHEN s.deathYear IS NULL THEN CONCAT(s.birthYear, '-alive')
             ELSE CONCAT(s.birthYear, '-', s.deathYear)
           END AS lifeSpan
    FROM directorStats s
    LEFT JOIN topGenres g ON s.primaryName = g.primaryName
)
SELECT primaryName, avgRating, filmCount, mostPopularGenre, lifeSpan
FROM (
    SELECT *,
           DENSE_RANK() OVER (ORDER BY avgRating DESC) AS rnk
    FROM finalResult
) ranked
WHERE rnk <= 5
""")

println("=== Top Director Performance ===")
topDirectors.show(false)

// ------------------------------------------------------------
// 5. Analysis C: TV series benchmarking
//    Business use case: catalog benchmarking and programming
// ------------------------------------------------------------
val tvSeries = basicsClean
  .filter($"titleType" === "tvSeries")
  .filter($"startYear".isNotNull)
  .withColumn("startYear", $"startYear".cast("int"))
  .withColumn("endYear", when($"endYear".isNull, lit(2024)).otherwise($"endYear".cast("int")))
  .select("tconst", "primaryTitle", "startYear", "endYear")
  .join(ratingsClean.withColumnRenamed("tconst", "ratings_tconst"), $"tconst" === $"ratings_tconst", "inner")
  .join(
    episodes.groupBy("parentTconst").agg(count("*").as("totalEpisodes")),
    $"tconst" === $"parentTconst",
    "left"
  )
  .withColumn("numYears", $"endYear" - $"startYear" + 1)
  .withColumn("avgEpisodesPerYear", $"totalEpisodes" / $"numYears")
  .select($"tconst", $"primaryTitle", $"startYear", $"endYear", $"averageRating", $"numVotes", $"avgEpisodesPerYear", $"numYears")

tvSeries.createOrReplaceTempView("tvSeries")

val topSeries = spark.sql("""
WITH topTVSeries AS (
    SELECT tconst, primaryTitle, startYear, endYear, averageRating, numVotes, avgEpisodesPerYear
    FROM tvSeries
    WHERE numYears >= 3 AND avgEpisodesPerYear >= 10
    ORDER BY averageRating DESC, avgEpisodesPerYear DESC, numVotes DESC
    LIMIT 3
),
remainingSeries AS (
    SELECT *
    FROM tvSeries
    WHERE tconst NOT IN (SELECT tconst FROM topTVSeries)
),
remainingAvg AS (
    SELECT AVG(averageRating) AS remainingAverage
    FROM remainingSeries
),
groupTwo AS (
    SELECT *
    FROM remainingSeries, remainingAvg
    WHERE numYears >= 3 AND numVotes >= 1000 AND averageRating >= remainingAverage * 1.25
),
groupTwoAvg AS (
    SELECT AVG(averageRating) AS group2Average
    FROM groupTwo
)
SELECT t.primaryTitle,
       CASE
         WHEN t.endYear = 2024 THEN CONCAT(t.startYear, '-present')
         ELSE CONCAT(t.startYear, '-', t.endYear)
       END AS runPeriod,
       ROUND(t.averageRating, 2) AS avgRating,
       ROUND(t.averageRating - g.group2Average, 2) AS ratingDiff
FROM topTVSeries t
CROSS JOIN groupTwoAvg g
ORDER BY avgRating DESC
""")

println("=== Top TV Series Benchmarking ===")
topSeries.show(false)

// ------------------------------------------------------------
// 6. Analysis D: French localization / drama share
//    Business use case: international content strategy
// ------------------------------------------------------------
val frenchTitles = akas
  .filter($"region" === "FR" && $"isOriginalTitle" === "0")
  .select($"titleId".as("tconst"))
  .distinct()
  .join(
    basicsClean
      .filter($"titleType" === "movie")
      .filter($"genres".isNotNull),
    "tconst"
  )
  .join(crewClean, "tconst")
  .join(ratingsClean, "tconst")
  .select("tconst", "primaryTitle", "genres", "director", "averageRating")

val nonDrama = basicsClean
  .filter($"titleType" === "movie")
  .filter($"genres".isNotNull)
  .filter(!$"genres".contains("Drama"))
  .join(crewClean, "tconst")
  .join(ratingsClean, "tconst")
  .select("tconst", "director", "averageRating")

val minNonDramaRatings = nonDrama
  .groupBy("director")
  .agg(min("averageRating").as("minRating"))

val frenchDramaData = frenchTitles
  .join(minNonDramaRatings, "director")
  .withColumn("genre", explode(split($"genres", ",")))
  .select("primaryTitle", "genre", "averageRating", "minRating")

frenchDramaData.createOrReplaceTempView("frenchDramaData")

val localizationInsight = spark.sql("""
SELECT ROUND(
    100.0 * SUM(CASE WHEN genre = 'Drama' AND minRating >= 6 THEN 1 ELSE 0 END) /
    SUM(CASE WHEN genre = 'Drama' THEN 1 ELSE 0 END),
    2
) AS dramaPercentage
FROM frenchDramaData
""")

println("=== French Localization Insight ===")
localizationInsight.show(false)