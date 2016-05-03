lazy val root = (project in file(".")).
  settings(
	name := "Wikidata Extraction Tool",
	version := "0.0.1-SNAPSHOT",
	organization := "ch.epfl.lsir",
	autoScalaLibrary := false,

    libraryDependencies += "org.wikidata.wdtk" % "wdtk-datamodel" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-dumpfiles" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-rdf" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-util" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-wikibaseapi" % "0.6.0",
    libraryDependencies += "org.wikidata.wdtk" % "wdtk-storage" % "0.6.0",
    libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  )


