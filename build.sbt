

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
    "org.apache.sqoop" % "sqoop" % sqoopV % "provided" excludeAll(ExclusionRule("org.kitesdk"),
                                                                 ExclusionRule("com.twitter")),
    "org.apache.hadoop" % "hadoop-common" % hadoopV % "provided",
    "ch.qos.logback" % "logback-classic" % logbackV,
   // "org.kitesdk" % "kite-data-hive" % "1.0.0" excludeAll(ExclusionRule("com.twitter")),
    "org.kitesdk" % "kite-data-core" % "1.0.0" excludeAll(ExclusionRule("org.kitesdk", "kite-data-hive"), ExclusionRule("com.twitter")),
    "org.kitesdk" % "kite-tools" % "1.0.0"  excludeAll(ExclusionRule("org.kitesdk", "kite-data-hive"), ExclusionRule("com.twitter")),
    "io.spray" % "spray-http" % sprayV,
    "com.typesafe.slick" %% "slick" % slickV,
    "com.typesafe" % "config" % typeSafeV,
    "mysql" % "mysql-connector-java" % mySqlV ,
    "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
    "org.mockito" % "mockito-core" % "1.8.5" % "test"
  ).map(_.force())
}

unmanagedResourceDirectories in Compile <+= baseDirectory

excludeFilter in unmanagedResources := HiddenFileFilter || "node_modules*" || "project*" || "target*" || "sqoop-*" || "lib*"
//|| "src/main/resources/*.conf"

test in assembly := {}
