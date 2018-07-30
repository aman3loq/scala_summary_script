import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.apache.spark.sql.expressions.Window

var D = spark.read.option("delimiter", ",").option("header", "true").csv("hdfs://10.10.1.200:8020/Exploree/gendata.csv")
D.show()
var E = spark.read.option("delimiter", ",").option("header", "true").csv("hdfs://10.10.1.200:8020/Exploree/event_meta.csv")
//val newColnames = Seq("event_id", "event_name")
//E = E.toDF(newColnames: _*)

D = D.withColumn("timestamp", from_unixtime(D.col("txn_ts") / 1000))
D = D.withColumn("doy", dayofyear(D.col("timestamp")))
D = D.withColumn("woy", weekofyear(D.col("timestamp")))

D.show()

//#Users # Events over timeline
val S12 = D.groupBy("woy").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("total_events"))

// Single-week users vs All-week users
var S16 = D.groupBy("user_id").agg(countDistinct("woy").alias("uniq_weeks_present"))
S16 = S16.filter(S16.col("uniq_weeks_present") === 1 || S16.col("uniq_weeks_present") === 8)
S16 = S16.withColumn("user_type_id", when(S16.col("uniq_weeks_present") === 1, "SINGLE_WEEK_USER").otherwise("ALL_WEEK_USER"))

val J = D.join(S16, Seq("user_id"))
J.groupBy("user_type_id").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("events_count"), countDistinct("event_id").alias("uniq_events")).show()


//Journeys...
val window = Window.partitionBy("user_id").orderBy("txn_ts")
var Jr = D.withColumn("rank", row_number().over(window))
Jr = Jr.withColumn("session_id", round(Jr.col("txn_ts") / 1000 / 60 / 30))


Jr = Jr.join(E, Seq("event_id"))
//val t0 = Jr.groupBy("user_id").agg(countDistinct("session_id").alias("total_sessions"))
//t0.groupBy("total_sessions").agg(countDistinct("user_id").alias("uniq_users"))

Jr = Jr.groupBy("user_id", "session_id", "woy").agg(concat_ws("/", collect_list("event_name")) as "session_path", sum(lit(1)).alias("path_length"))
Jr = Jr.join(J.select("user_id", "user_type_id"), Seq("user_id"))
Jr = Jr.dropDuplicates()
//Jr.write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/journey_paths.csv")

Jr = spark.read.option("delimiter", ",").option("header", "true").csv("hdfs://10.10.1.200:8020/Exploree/journey_paths.csv")

val TopPaths = Jr.groupBy("session_path").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("uniq_sessions")).orderBy(desc("uniq_users")).limit(5000)
TopPaths.coalesce(1).write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/TopPaths_5000.csv")

val LongestPathSingleWeekUsers = Jr.filter(J.col("user_type_id") === "SINGLE_WEEK_USER").groupBy("session_path").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("uniq_sessions")).orderBy(desc("uniq_users")).limit(5000)
LongestPathSingleWeekUsers.coalesce(1).write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/LongestPathSingleWeekUsers_5000.csv")

val LongestPathAllWeekUsers = Jr.filter(J.col("user_type_id") === "ALL_WEEK_USER").groupBy("session_path").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("uniq_sessions")).orderBy(desc("uniq_users")).limit(5000)
LongestPathAllWeekUsers.coalesce(1).write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/LongestPathAllWeekUsers_5000.csv")

val paths_groups_summary = Jr.filter(Jr.col("user_type_id") === "ALL_WEEK_USER").groupBy("session_path").agg(countDistinct("user_id").alias("uniq_users"), sum(lit(1)).alias("uniq_sessions")).orderBy(desc("uniq_users"))
paths_groups_summary.coalesce(1).write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/paths_groups_summary.csv")

//S18
var t0 = Jr.groupBy("user_type_id", "woy").agg(countDistinct("session_id").alias("total_sessions"))
t0.groupBy("user_type_id").agg(avg("total_sessions")).show()

//Inc/Dec Features...
val event_trends = Jr.groupBy("event_name", "woy").agg(countDistinct("user_id").alias("uniq_users"))
event_trends.coalesce(1).write.mode("overwrite").option("header", true).csv("hdfs://10.10.1.200:8020/Exploree/event_trends.csv")



