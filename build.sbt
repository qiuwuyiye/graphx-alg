name := "NMF_project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.0" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.2.0"
)

/*
assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith("Log$Logger.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
*/