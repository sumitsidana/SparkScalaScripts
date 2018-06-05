name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.6.0",
"org.apache.spark" %% "spark-sql" % "1.6.0",
"org.apache.spark" %% "spark-mllib" % "1.6.0"
)
// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
 "http://repo.typesafe.com/typesafe/simple/maven-releases/"
