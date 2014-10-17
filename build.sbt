import AssemblyKeys._

import NativePackagerKeys._

assemblySettings

net.virtualvoid.sbt.graph.Plugin.graphSettings


name := "cert-log-manager"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.0.0" % "provided"

//libraryDependencies += ("org.apache.flume" % "flume-ng-sdk" % "1.5.0.1").exclude("commons-collections","commons-collections")
//.exclude("commons-logging","commons-logging")


//libraryDependencies += ("org.apache.flume" % "flume-ng-sdk" % "1.4.0").exclude("commons-collections","commons-collections")
//.exclude("commons-logging","commons-logging")



libraryDependencies += ("org.apache.spark" %% "spark-streaming-flume" % "1.0.0")
.exclude("org.eclipse.jetty.orbit","javax.transaction")
.exclude("org.eclipse.jetty.orbit","javax.mail.glassfish")
.exclude("com.esotericsoftware.minlog","minlog")
.exclude("commons-beanutils","commons-beanutils")
.exclude("commons-collections","commons-collections")

//exclude("com.twitter","chill-java")// exclude("com.twitter","chill_2.10") exclude("commons-configuration","commons-configuration") exclude("org.eclipse.jetty","jetty-plus")
//.exclude("org.eclipse.jetty.orbit","javax.servlet")
//.exclude("org.eclipse.jetty.orbit","javax.activation")

libraryDependencies += "log4j" % "log4j" % "1.2.16"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.13"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Maven Repository" at "http://repo.maven.apache.org/maven2"



