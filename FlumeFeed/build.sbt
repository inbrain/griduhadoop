name := "FlumeFeedX"

version := "0.1"

scalaVersion := "2.11.12"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"

assemblyJarName in assembly := "fat.jar"