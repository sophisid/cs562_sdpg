// Define common dependencies and versions
import Dependencies._

ThisBuild / scalaVersion     := "2.12.15"   // Use Scala 2.12 for compatibility with Neo4j Spark Connector
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

// Uncomment and modify if needed for Windows
// ThisBuild / javaHome := Some(file("C:\\Program Files\\Java\\jdk-11.0.17"))

lazy val root = (project in file("."))
  .settings(
    name := "schemadiscovery",
    
    // Add library dependencies
    libraryDependencies ++= Seq(
      munit % Test,          // Testing dependency
      sparkCore,             // Spark Core
      sparkSql,              // Spark SQL
      mllib,                 // Spark MLlib
      "org.neo4j.driver" % "neo4j-java-driver" % "4.4.10",   // Neo4j Java driver
      "org.apache.spark" %% "spark-hive" % "3.2.4",  
    ),

    // Add Neo4j Maven repository to resolve the connector
    resolvers += "Neo4j Maven Repository" at "https://neo4j-contrib.github.io/maven/",

    // Ensure the application forks when running to use the Java options
    Compile / run / fork := true
  )
