name := "spark-kafka-parquet-example"

version := "0.2"

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Typesafe repository releases" at "https://repo.typesafe.com/typesafe/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Twitter Repo" at "https://maven.twttr.com",
  "eaio.com" at "https://eaio.com/maven2"
)

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                          % "1.4.1",
  "ch.qos.logback"    % "logback-classic"                 % "1.2.3",
  "org.apache.spark"  % "spark-streaming_2.12"            % sparkVersion,
  "org.apache.spark"  % "spark-sql_2.12"            % sparkVersion,
  "org.apache.spark"  % "spark-streaming-kafka-0-10_2.12" % sparkVersion
)

val myAssemblySettings = Seq(
  ThisBuild / assemblyMergeStrategy := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.last
  }
)

lazy val commonSettings = Seq(
  organization := "com.justinrmiller",
  scalaVersion := "2.12.8",
  test in assembly := {},
  fork in run := true
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(myAssemblySettings: _*).
  settings(
    mainClass in assembly := Some("com.justinrmiller.sparkstreamingexample.Main")
  )

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
