import sbt.Keys.libraryDependencies

lazy val commonSettings = Seq(
  name := "MyMovieRecommendation",
  version := "0.1",
  scalaVersion := "2.12.12"
)
lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "MyMovieRecommendation",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
mainClass in assembly := Some("Recommender")
//jarName in assembly := "movieRecommender.jar"