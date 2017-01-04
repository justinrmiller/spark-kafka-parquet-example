name := "spark-kafka-parquet-example"

version := "0.1"

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Twitter Repo" at "http://maven.twttr.com",
  "eaio.com" at "http://eaio.com/maven2"
)

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                          % "1.3.0",
  "ch.qos.logback"    % "logback-classic"                 % "1.1.3",
  "org.apache.spark"  % "spark-streaming_2.11"            % sparkVersion,
  "org.apache.spark"  % "spark-streaming-kafka-0-10_2.11" % sparkVersion
)

val myAssemblySettings = Seq(
  assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
      case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  }
)

lazy val commonSettings = Seq(
  organization := "com.justinrmiller",
  scalaVersion := "2.11.8",
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
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
