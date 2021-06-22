
lazy val hadoopVersion = "3.0.1"
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.12"
    )),
    name := "avroplay",
    version := "0.0.1",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue,

    avroScalaSpecificCustomTypes in Compile := {
      avrohugger.format.SpecificRecord.defaultTypes.copy(
        record = avrohugger.types.ScalaCaseClassWithSchema)
    },

    libraryDependencies ++= Seq(

        "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test",
        "org.apache.avro" % "avro" % "1.8.2" % "provided",
        "org.apache.hadoop" % "hadoop-client"      % hadoopVersion,
        "org.apache.hadoop" % "hadoop-common"      % hadoopVersion,
        "org.apache.hadoop" % "hadoop-hdfs"        % hadoopVersion,
        "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test,
        "org.scalacheck" %% "scalacheck" % "1.13.5" % Test
    ),
    // Align dependencies to spark 2.4 or dependency hell will follow
    dependencyOverrides ++= Seq( 
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"
    )

    
  )