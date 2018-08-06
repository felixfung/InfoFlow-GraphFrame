name                := "InfoFlow"
version             := "1.0"
scalaVersion        := "2.11.7" //"2.12.1"
parallelExecution   := false
libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.1.1",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test",
        "org.apache.spark" %% "spark-sql" % "2.1.1",
        "org.apache.spark" %% "spark-graphx" % "2.1.1",
        "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",
        "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
)
