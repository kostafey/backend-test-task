name := "backend-test-task"

version := "2.0"

scalaVersion := "2.13.10"

val zioVersion = "2.0.10"

libraryDependencies ++= Seq(
  "com.beachape" %% "enumeratum" % "1.7.2",
  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
