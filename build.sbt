name := "sqoop-service"

version       := "0.1"

scalaVersion  := "2.10.4"

scalacOptions := Seq("-unchecked", "+deprecation", "-encoding", "utf8", "-feature")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
//  "cloudera" at "https://repository.cloudera.com/content/repositories/releasess/",
//  "cloudera public" at "https://repository.cloudera.com/artifactory/public/",
  "cloudera repo" at "https://repository.cloudera.com/artifactory/repo"
)

libraryDependencies ++= {
  val akkaV = "2.2.3"
  val sprayV = "1.2.0"
  val slickV = "1.0.1"
  val typeSafeV = "1.2.1"
  val hadoopV = "2.6.0"
  val sqoopV = "1.4.5-cdh5.3.2"
  val mySqlV = "5.1.25"
  val logbackV= "1.0.13"
  Seq(
    "org.apache.sqoop" % "sqoop" % sqoopV % "provided",
    "org.apache.hadoop" % "hadoop-common" % hadoopV % "provided",
    "ch.qos.logback" % "logback-classic" % logbackV excludeAll ExclusionRule(organization = "org.slf4j"),
    "io.spray" % "spray-http" % sprayV excludeAll ExclusionRule(organization = "org.slf4j"),
    "com.typesafe.slick" %% "slick" % slickV excludeAll ExclusionRule(organization = "org.slf4j"),
    "com.typesafe" % "config" % typeSafeV excludeAll ExclusionRule(organization = "org.slf4j"),
    "mysql" % "mysql-connector-java" % mySqlV,
    "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
    "org.mockito" % "mockito-core" % "1.8.5" % "test"
  ).map(_.force())
}

// ~= { _.map(_.excludeAll(ExclusionRule(organization = "org.slf4j"))) }
//sbt-revolver plugin allows restarting the application when files change (including angular files in the /app folder)
//Just run sbt or activator with the command `~ re-start` instead of `run`
//Revolver.settings

unmanagedResourceDirectories in Compile <+= baseDirectory

excludeFilter in unmanagedResources := HiddenFileFilter || "node_modules*" || "project*" || "target*" || "sqoop-*" || "lib"

test in assembly := {}