import sbt._

object Dependencies {
  val scalaVer = "2.13.12" 
  val munitVer = "0.7.29"
  val sparkVer = "3.2.4"

  val munit = "org.scalameta" %% "munit" % munitVer
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVer
}