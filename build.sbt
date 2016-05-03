lazy val commonSettings = Seq(
	version := "0.0.1-SNAPSHOT",
	organization := "ch.epfl.lsir"	
)


lazy val extracting = (project in file("extracting-tool")).
  settings(
	name := "Wikidata Extraction Tool",

    libraryDependencies += "org.wikidata.wdtk" % "wdtk-datamodel" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-dumpfiles" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-rdf" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-util" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-wikibaseapi" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-storage" % "0.6.0",
    libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  )



lazy val processing = (project in file("processing-tool")).
  settings(
	name := "Taxonomy Analyzer",
	scalaVersion := "2.11.7",
	crossScalaVersions := Seq("2.10.5", "2.11.7"),
	libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0",
	libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0",
	libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"
	)



lazy val root = (project in file("."))
  .aggregate(extracting, processing)
  .settings(commonSettings: _*)

