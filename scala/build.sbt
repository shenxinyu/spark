lazy val root = (project in file(".")).
    settings(
        name := "Rank Project",
        version := "1.0",
        scalaVersion := "2.10.4",
        libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1",
        libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1",
        libraryDependencies += "redis.clients" % "jedis" % "2.7.2"
    )
