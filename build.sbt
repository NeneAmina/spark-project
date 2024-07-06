ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2", // Ajout de la dépendance Spark SQL
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2" ,// Kafka integration dependency
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.12.3",
      "org.postgresql" % "postgresql" % "42.2.23"
    )
  )