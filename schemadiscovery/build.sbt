import Dependencies._

ThisBuild / scalaVersion     := scalaVer
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
      "org.neo4j.driver" % "neo4j-java-driver" % "4.4.10"    ),

    // Ensure the application forks when running to use the Java options
    Compile / run / fork := true
  )
