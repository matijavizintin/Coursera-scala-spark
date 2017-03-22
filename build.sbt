name := "Coursera-scala-spark"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"
libraryDependencies += "junit" % "junit" % "4.10" % "test"
