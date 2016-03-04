import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

object readGraph {
  
  var verticesFile, edgesFile: String = null
  
  //initialize Spark
  val conf = new SparkConf().setAppName("readGraph").setMaster("local")
  val sc = new SparkContext(conf)

  //create graph from edges ignoring single node classes
  val graphFromEdges = true
  
  //read from csv files
  lazy val verticesRDD = sc.textFile(verticesFile).map(line => line.split(",")).map(line => (line(0).toString.substring(1).toInt:VertexId, line(1).toString()))
  lazy val edgesRDD = sc.textFile(edgesFile).map(line => line.split(",")).map(line => Edge(line(0).toString.substring(1).toInt:VertexId, line(1).toString.substring(1).toInt:VertexId, "subclass"))

  //create graph
  lazy val graph = if (graphFromEdges) Graph.fromEdges(edgesRDD, "No Label") else Graph(verticesRDD, edgesRDD, "No Label")
  
  //outDegree=0 => root class
  lazy val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter{case (_, neighbors) => neighbors.size==0}.leftJoin(verticesRDD){case (id, _, label) => (id, label.getOrElse("No Label"))}
  //inDegree=0 => leaf class
  lazy val leafClasses = graph.collectNeighborIds(EdgeDirection.In).filter{case (_, neighbors) => neighbors.size==0}.leftJoin(verticesRDD){case (id, _, label) => (id, label.getOrElse("No Label"))} 
  
  def firstLevelStatistics {
	  //vertices = classes, edges = subclass relations
	  println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges)

	  //graph from edges doesn't have labeled vertices => join with vertices RDD
	  if (graphFromEdges)
		  println("Classes without label: " + graph.vertices.join(verticesRDD).filter{ case (_, (_, label)) => label.equals("No Label")}.count())
	  else
		  println("Classes without label: " + graph.vertices.filter{case (_, label) => label.equals("No Label")}.count())    
  }
  
  def hierarchyStatistics {
    //root /\ leaf => single node
    val singleNodeClasses = rootClasses.intersection(leafClasses).map{case (_, (id, label)) => (id, label)}
    //root - leaf => root but not single node
    val rootNotSingleNodeClasses = rootClasses.subtract(leafClasses).map{case (_, (id, label)) => (id, label)}
    
    //print statistics
    println("Root Classes: " + rootClasses.count())
    println("Leaf Classes: " + leafClasses.count())
    println("Single Node Classes: " + singleNodeClasses.count())    
    println("Root not Single Node Classes: " + rootNotSingleNodeClasses.count())

    //write classes to files         
    val writer1 = new PrintWriter(new File("singleNodeClasses.csv" ))
    val writer2 = new PrintWriter(new File("rootNotSingleNodeClasses.csv" ))
    singleNodeClasses.collect.foreach{case (id, label) => writer1.write("http://www.wikidata.org/wiki/Q" + id + " , " + label + "\n")}
    rootNotSingleNodeClasses.collect.foreach{case (id, label) => writer2.write("http://www.wikidata.org/wiki/Q" + id + " , " + label + "\n")}
    writer1.close()
    writer2.close()
  }
  
  def createSubgraphs {
	  //compute subgraphs
	  val subgraphs = graph.connectedComponents().vertices.join(verticesRDD).leftOuterJoin(rootClasses).map{ case ((id, ((graphId, label), Some((_,isRoot))))) => (graphId, (id, label, true)) ; case ((id, ((graphId, label), None))) => (graphId, (id, label, false))}.groupByKey.collect
    
	  //write metadata about subgraphs
    val metadata = new PrintWriter(new File("subgraphs.txt" ))
    metadata.write("#Subgraphs: " + subgraphs.length + "\n")
    subgraphs.foreach{ case (graphId, vertices) => 
    metadata.write("======Graph " + graphId + " (#Roots: " + vertices.count{case (_, _, isRoot) => isRoot} + ", #Classes: "+vertices.size+")======\n")
    vertices.foreach{ case (_, label, isRoot) => if (isRoot) metadata.write(label + "\n")}}
    metadata.close()
  }
  
  def main(args: Array[String]) {

	  if (args.length == 2) {
		  verticesFile = args(0)
		  edgesFile = args(1)      
	   
		  createSubgraphs
	  }
  }
}