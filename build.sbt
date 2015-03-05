name := "sqoop-service"

version       := "0.1"

scalaVersion  := "2.10.4"

scalacOptions := Seq("-unchecked", "+deprecation", "-encoding", "utf8", "-feature")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases/"
)

libraryDependencies ++= {
  val akkaV = "2.2.3"
  val sprayV = "1.2.0"
  val slickV = "1.0.1"
  val typeSafeV = "1.2.1"
  val hadoopV = "2.6.0"
  val sqoopV = "1.4.5-cdh5.3.2"
  val log4jV = "1.2.14"
  val mySqlV = "5.1.25"
  Seq(
    "org.apache.sqoop" % "sqoop" % sqoopV % "provided",
    "org.apache.hadoop" % "hadoop-common" % hadoopV % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "org.slf4j" % "slf4j-simple" % "1.7.5",
    "io.spray" % "spray-http" % sprayV,
    "com.typesafe.slick" %% "slick" % slickV,
    "com.typesafe" % "config" % typeSafeV,
    "mysql" % "mysql-connector-java" % mySqlV % "provided"
  )
}

//sbt-revolver plugin allows restarting the application when files change (including angular files in the /app folder)
//Just run sbt or activator with the command `~ re-start` instead of `run`
//Revolver.settings

unmanagedResourceDirectories in Compile <+= (baseDirectory)

excludeFilter in unmanagedResources := HiddenFileFilter || "node_modules*" || "project*" || "target*" || "sqoop-*" || "lib"
