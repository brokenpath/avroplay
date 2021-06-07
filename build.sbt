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

    libraryDependencies ++= Seq(

        "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    )

    
  )