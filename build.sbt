ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "sql42",
    libraryDependencies ++= Seq(
      "org.apache.calcite" % "calcite-core" % "1.26.0",
      "org.scala-lang" % "scala-library" % "2.13.8",
      "org.scalatest" %% "scalatest" % "3.2.11" % Test,
      "org.mockito" %% "mockito-scala" % "1.17.5" % Test,
      "org.apache.spark" %% "spark-core" % "3.1.3",
      "org.apache.spark" %% "spark-sql" % "3.1.3",
    )
  )

