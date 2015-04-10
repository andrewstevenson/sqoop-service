

name := "sqoop-service"

version       := "0.1"

scalaVersion  := "2.10.4"

scalacOptions := Seq("-unchecked", "+deprecation", "-encoding", "utf8", "-feature")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
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
  val kiteSDKV= "1.0.0"
  Seq(
    "org.apache.sqoop" % "sqoop" % sqoopV excludeAll(ExclusionRule("org.kitesdk"),ExclusionRule("com.twitter"), ExclusionRule("org.slf4j")),
    "org.apache.hadoop" % "hadoop-common" % hadoopV % "provided" excludeAll(ExclusionRule("org.slf4j")),
    "ch.qos.logback" % "logback-classic" % logbackV % "provided",
    //once Kite 1.1 is out set kite sdk to provided.
    "org.kitesdk" % "kite-data-mapreduce" % kiteSDKV,// excludeAll(ExclusionRule("org.kitesdk", "kite-data-hive"), ExclusionRule("com.twitter"), ExclusionRule("org.slf4j")),
    "org.kitesdk" % "kite-tools" % kiteSDKV  excludeAll(ExclusionRule("org.kitesdk", "kite-data-hive"), ExclusionRule("com.twitter"), ExclusionRule("org.slf4j")),
    "io.spray" % "spray-http" % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-servlet"     % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-routing"     % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-testkit"     % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-client"      % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-util"        % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-caching"     % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "io.spray" %   "spray-can" % sprayV excludeAll(ExclusionRule("org.slf4j")),
    "com.typesafe.akka"   %%  "akka-slf4j"        % "2.1.4" excludeAll(ExclusionRule("org.slf4j")),
    "net.liftweb" %% "lift-json" % "2.5.1" excludeAll(ExclusionRule("org.slf4j")),
    "com.typesafe.slick" %% "slick" % slickV excludeAll(ExclusionRule("org.slf4j")),
    "com.typesafe" % "config" % typeSafeV excludeAll(ExclusionRule("org.slf4j")),
    "mysql" % "mysql-connector-java" % mySqlV excludeAll(ExclusionRule("org.slf4j")),
    "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
    "org.mockito" % "mockito-core" % "1.8.5" % "test"
  ).map(_.force())
}

unmanagedResourceDirectories in Compile <+= baseDirectory

excludeFilter in unmanagedResources := HiddenFileFilter || "node_modules*" || "project*" || "target*" || "sqoop-*"// || "lib*"
//|| "src/main/resources/*.conf"

test in assembly := {}
