import sbt.ExclusionRule
name := "accathon-poc"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.7"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8"


unmanagedBase <<= baseDirectory { base => base / "libs" }

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
