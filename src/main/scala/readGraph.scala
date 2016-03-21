import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
import java.util.HashMap

object readGraph {
  
  var verticesFile, edgesFile: String = null

  val separator = "\t"
  val newline = "\n"
  
  //initialize Spark
  val conf = new SparkConf().setAppName("readGraph").setMaster("local")
  val sc = new SparkContext(conf)

  //create graph from edges ignoring single node classes
  val graphFromEdges = true
  
  //remove classes without label
  val removeClassesWithoutLabel = true
  
  //read from csv files
  lazy val verticesRDD = sc.textFile(verticesFile).map(line => line.split(separator)).map(line => (line(0).toString.substring(1).toLong, (line(1).toString, line(2).toInt)))
  lazy val edgesRDD = sc.textFile(edgesFile).map(line => line.split(separator)).map(line => Edge(line(0).toString.substring(1).toLong, line(1).toString.substring(1).toLong, "subclass"))

  //create graph
  lazy val initialGraph = if (graphFromEdges) Graph.fromEdges(edgesRDD, ("No Label", 0)).joinVertices(verticesRDD){case (_, (_, _), (label, numOfInstances)) => (label, numOfInstances)} else Graph(verticesRDD, edgesRDD, ("No Label", 0))
  lazy val graph = if (removeClassesWithoutLabel) initialGraph.subgraph(vpred = (id, attr) => attr._1 != "No Label") else initialGraph
  
  //outDegree=0 => root class
  lazy val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter{case (_, neighbors) => neighbors.size==0}.join(graph.vertices).map{case (id, (_, (label, numOfInstances))) => (id, (label, numOfInstances))}
  //inDegree=0 => leaf class
  lazy val leafClasses = graph.collectNeighborIds(EdgeDirection.In).filter{case (_, neighbors) => neighbors.size==0}.join(graph.vertices).map{case (id, (_, (label, numOfInstances))) => (id, (label, numOfInstances))} 

  //compute subgraphs
	lazy val subgraphs = graph.connectedComponents().vertices.join(verticesRDD).leftOuterJoin(rootClasses).map{ case ((id, ((graphId, (label, numOfInstances)), isRoot))) => (graphId, (id, (label, numOfInstances, isRoot.exists(_ => true))))}.groupByKey

  def firstLevelStatistics {
	  //vertices = classes, edges = subclass relations
	  println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges, "Instances: "+ graph.vertices.map{case ((_, (_, numOfInstances))) => numOfInstances}.reduce(_ + _))
	  
		println("Classes without label: " + graph.vertices.filter{case (_, (label, _)) => label.equals("No Label")}.count())
		println("Classes without instanses: " + graph.vertices.filter{case (_, (_, numOfInstances)) => numOfInstances.equals(0)}.count())    
  }
  
  def hierarchyStatistics {
    //root /\ leaf => single node
    val singleNodeClasses = rootClasses.intersection(leafClasses)
    //root - leaf => root but not single node
    val rootNotSingleNodeClasses = rootClasses.subtract(leafClasses)
    
    //print statistics
    println("Root Classes: " + rootClasses.count())
    println("Leaf Classes: " + leafClasses.count())
    println("Single Node Classes: " + singleNodeClasses.count())    
    println("Root not Single Node Classes: " + rootNotSingleNodeClasses.count())

    //write classes to files         
    val writer1 = new PrintWriter(new File("singleNodeClasses.csv" ))
    val writer2 = new PrintWriter(new File("rootNotSingleNodeClasses.csv" ))
    singleNodeClasses.collect.foreach{case (id, (label, numOfInstances)) => writer1.write("http://www.wikidata.org/wiki/Q" + id + " , " + label + newline)}
    rootNotSingleNodeClasses.collect.foreach{case (id, (label, numOfInstances)) => writer2.write("http://www.wikidata.org/wiki/Q" + id + " , " + label + newline)}
    writer1.close()
    writer2.close()
  }
  
  def subgraphsStatistics {
	  
	  //write metadata about subgraphs
    val metadata1 = new PrintWriter(new File("subgraphs.txt" ))
    metadata1.write("#Subgraphs: " + subgraphs.count + newline)
    subgraphs.foreach{ case (graphId, vertices) => 
    metadata1.write("======Graph " + graphId + " (#Roots: " + vertices.count{case (_, (_, _, isRoot)) => isRoot} + ", #Classes: " + vertices.size + ")======\n")
    vertices.foreach{ case (_, (label, _, isRoot)) => if (isRoot) metadata1.write(label + newline)}}
    metadata1.close()
    
    val metadata2 = new PrintWriter(new File("subgraphs.tsv" ))
    subgraphs.foreach{ case (graphId, vertices) => 
    metadata2.write("G" + graphId + separator + vertices.size + separator + vertices.map(f => (f._2._2)).sum + newline)}
    metadata2.close()
  }

  def instancesStatistics {
    val writer = new PrintWriter(new File("instancesPerClass.tsv" ))
    graph.vertices.collect.foreach{case (_, (label, numOfInstances)) => writer.write(label + separator + numOfInstances + newline)}
    writer.close
  }
  
  def extractSubgraph(id:Integer) {
    val subgraph = subgraphs.filter{case (graphId, _) => graphId==id}.map{case (_, vertices) => vertices}.flatMap(f=>f)
    
    val writer1 = new PrintWriter(new File("Graph" + id + "_Edges.tsv" ))
    graph.joinVertices(subgraph){case (_, (label, numOfInstances), _) => (label, numOfInstances)}.edges.collect.foreach{edge => writer1.write(edge.srcId + separator + edge.dstId + newline) }
    writer1.close()
    
    val writer2 = new PrintWriter(new File("Graph" + id + "_Vertices.tsv" ))
    subgraph.collect.foreach{case (id, (label, numOfIsntances, isRoot)) => writer2.write(id + separator + label + separator + numOfIsntances + separator + isRoot + newline)}
    writer2.close()    
  }
  
  def main(args: Array[String]) {

	  if (args.length == 2) {
		  verticesFile = args(0)
		  edgesFile = args(1)      
	   
		  //firstLevelStatistics
		  //hierarchyStatistics
		  //subgraphsStatistics
		  //instancesStatistics
		  extractSubgraph(3)
	  }
  }
}