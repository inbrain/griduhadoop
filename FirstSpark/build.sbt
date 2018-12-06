name := "FirstSpark"

version := "0.1"

scalaVersion := "2.10.7"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
libraryDependencies += "commons-net" % "commons-net" % "3.6"

assemblyJarName in assembly := "sparklab.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}


//for spark 1.6 


// for spark sqlcontext


// this is the show stealer,this is the dependency 