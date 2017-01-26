name := "untitled"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Cloudera Repos" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.cloudera.sparkts" % "sparkts" % "0.3.0"
)