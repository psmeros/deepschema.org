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
  
  case class Vertex(val label:String, val numOfInstances:Int, val isRoot:Boolean)
  
  //initialize Spark
  val conf = new SparkConf().setAppName("readGraph").setMaster("local")
  val sc = new SparkContext(conf)

  //create graph from edges ignoring single node classes
  val graphFromEdges = true
  
  //remove classes without label
  val removeClassesWithoutLabel = true
  
  //read from csv files
  lazy val verticesRDD = sc.textFile(verticesFile).map(line => line.split(separator)).map(line => (line(0).toString.substring(1).toLong, Vertex(line(1).toString, line(2).toInt, false)))
  lazy val edgesRDD = sc.textFile(edgesFile).map(line => line.split(separator)).map(line => Edge(line(0).toString.substring(1).toLong, line(1).toString.substring(1).toLong, "subclass"))

  //create graph
  lazy val initialGraph = if (graphFromEdges) Graph.fromEdges(edgesRDD, Vertex("No Label", 0, true)).joinVertices(verticesRDD){case (_, _, vertex) => vertex} else Graph(verticesRDD, edgesRDD, Vertex("No Label", 0, true))
  lazy val graph = if (removeClassesWithoutLabel) initialGraph.subgraph(vpred = (id, vertex) => vertex.label != "No Label") else initialGraph
  
  //outDegree=0 => root class
  lazy val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter{case (_, neighbors) => neighbors.size==0}.join(graph.vertices).map{case (id, (_, vertex)) => (id, vertex)}
  //inDegree=0 => leaf class
  lazy val leafClasses = graph.collectNeighborIds(EdgeDirection.In).filter{case (_, neighbors) => neighbors.size==0}.join(graph.vertices).map{case (id, (_, vertex)) => (id, vertex)} 

  //compute subgraphs
	lazy val subgraphs = graph.connectedComponents().vertices.join(verticesRDD).leftOuterJoin(rootClasses).map{ case ((id, ((graphId, vertex), rootVertex))) => (graphId, (id, Vertex(vertex.label, vertex.numOfInstances, rootVertex.exists(_ => true))))}.groupByKey

  def firstLevelStatistics {
	  //vertices = classes, edges = subclass relations
	  println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges, "Instances: "+ graph.vertices.map{case ((_, vertex)) => vertex.numOfInstances}.reduce(_ + _))
	  
		println("Classes without label: " + graph.vertices.filter{case (_, vertex) => vertex.label.equals("No Label")}.count())
		println("Classes without instanses: " + graph.vertices.filter{case (_, vertex) => vertex.numOfInstances.equals(0)}.count())    
  }
  
  def hierarchyStatistics {
    //root /\ leaf => single node
    val singleNodeClasses = rootClasses.intersection(leafClasses)
    //root - leaf => root but not single node
    val rootNotSingleNodeClasses = rootClasses.subtract(leafClasses)
    
    //print statistics
    println("Root Classes: " + rootClasses.count)
    println("Leaf Classes: " + leafClasses.count)
    println("Single Node Classes: " + singleNodeClasses.count)    
    println("Root not Single Node Classes: " + rootNotSingleNodeClasses.count)
  }
  
  def subgraphsStatistics {
	  
	  //write metadata about subgraphs
    val metadata1 = new PrintWriter(new File("subgraphs.txt" ))
    metadata1.write("#Subgraphs: " + subgraphs.count + newline)
    subgraphs.collect.foreach{ case (graphId, vertices) => 
    metadata1.write("======Graph " + graphId + " (#Roots: " + vertices.count{case (_, vertex) => vertex.isRoot} + ", #Classes: " + vertices.size + ")======\n")
    vertices.foreach{ case (_, vertex) => if (vertex.isRoot) metadata1.write(vertex.label + newline)}}
    metadata1.close()
    
    val metadata2 = new PrintWriter(new File("subgraphs.tsv" ))
    subgraphs.collect.foreach{ case (graphId, vertices) => 
    metadata2.write("G" + graphId + separator + vertices.size + separator + vertices.map{ case (_, vertex) => vertex.numOfInstances}.sum + newline)}
    metadata2.close()
  }

  def instancesStatistics {
    val writer = new PrintWriter(new File("instancesPerClass.tsv" ))
    graph.vertices.collect.sortBy{case (id, vertex) => vertex.numOfInstances}(Ordering[Int].reverse).foreach{case (_, vertex) => writer.write(vertex.label + separator + vertex.numOfInstances + newline)}
    writer.close
  }
  
  def extractSubgraph(id:Int) {    
    val subgraph = Graph(subgraphs.filter{case (graphId, _) => graphId==id}.map{case (_, vertices) => vertices}.flatMap(f=>f), edgesRDD, Vertex("No Label", 0, true)).subgraph(vpred = (id, vertex) => vertex.label != "No Label")
    
    val writer1 = new PrintWriter(new File("Graph" + id + "_Edges.tsv" ))
    subgraph.edges.collect.foreach{edge => writer1.write(edge.srcId + separator + edge.dstId + newline) }
    writer1.close()
    
    val writer2 = new PrintWriter(new File("Graph" + id + "_Vertices.tsv" ))
    subgraph.vertices.collect.foreach{case (id, vertex) => writer2.write(id + separator + vertex.label + separator + vertex.numOfInstances + separator + vertex.isRoot + newline)}
    writer2.close()    

    val writer3 = new PrintWriter(new File("Graph" + id + "_Roots.tsv" ))
    subgraph.vertices.filter{case (_, vertex) => vertex.isRoot}.collect.foreach{case (id, vertex) => writer3.write(id + separator + vertex.label + newline)}
    writer3.close()        
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