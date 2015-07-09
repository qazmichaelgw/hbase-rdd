name := "6spark"

organization := "6estates"

version := "0.0.1"

scalaVersion := "2.10.4"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls"
)

org.scalastyle.sbt.ScalastylePlugin.Settings

resolvers ++= Seq(
  "nexus" at "http://repos.6estates.com/nexus/content/groups/public"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.98.4-hadoop2" % "compile",
  "org.apache.hbase" % "hbase-common" % "0.98.4-hadoop2" % "compile",
  "org.apache.hbase" % "hbase-server" % "0.98.4-hadoop2" % "compile",
  "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.9-hadoop2" exclude("javax.servlet", "servlet-api"),
  "org.apache.hbase" % "hbase-hadoop2-compat" % "0.98.9-hadoop2" exclude("javax.servlet", "servlet-api"),
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "com.framework" % "framework-db-hbase" % "1.1.0-SNAPSHOT" % "compile"
)