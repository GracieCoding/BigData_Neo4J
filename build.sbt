name := "untitled3"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

fork in run := true

resolvers ++= Seq("anormcypher" at "http://repo.anormcypher.org/", "TypesafeReleases" at "http://repo.typesafe.com/typesafe/releases/")

libraryDependencies ++= Seq("org.anormcypher" %% "anormcypher" % "0.6.0")

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

libraryDependencies += "org.scala-lang" % "scala-actors" % "2.11.6"