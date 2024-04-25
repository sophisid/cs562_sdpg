import Dependencies._

ThisBuild / scalaVersion     := scalaVer
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "schemadiscovery",
    libraryDependencies ++= Seq(
      munit % Test,
      sparkCore 
    )
  )
