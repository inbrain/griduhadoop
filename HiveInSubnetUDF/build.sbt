name := "HiveSubnetUDF"

version := "0.1"

scalaVersion := "2.11.12"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies += "commons-net" % "commons-net" % "3.6"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.1"
libraryDependencies += "org.apache.hive" % "hive-exec" % "3.1.1"

assemblyJarName in assembly := "udf.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
