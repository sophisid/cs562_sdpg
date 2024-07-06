import Dependencies._

ThisBuild / scalaVersion     := scalaVer
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / javaHome         := Some(file("C:\\Program Files\\Java\\jdk-11.0.17"))

onLoadMessage := {
  s"Using Java Home: ${((ThisBuild / javaHome).value.map(_.getAbsolutePath)).getOrElse("Not Set")}"
} //uncomment for windows

lazy val root = (project in file("."))
  .settings(
    name := "schemadiscovery",
    libraryDependencies ++= Seq(
      munit % Test,
      sparkCore,   
      sparkSql,
      mllib
    ),

    // Set Java options for running applications
    fork in run := true,
    javaOptions in run ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--illegal-access=warn" // This option will log rather than throw when illegal accesses occur
    ) //uncomment for windows
  )
