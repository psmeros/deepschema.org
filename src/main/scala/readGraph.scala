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
  
  //initialize Spark
  val conf = new SparkConf().setAppName("readGraph").setMaster("local")
  val sc = new SparkContext(conf)

  //create graph from edges ignoring single node classes
  val graphFromEdges = true
  
  //read from csv files
  lazy val verticesRDD = sc.textFile(verticesFile).map(line => line.split(separator)).map(line => (line(0).toString.substring(1).toLong, (line(1).toString, line(2).toInt)))
  lazy val edgesRDD = sc.textFile(edgesFile).map(line => line.split(separator)).map(line => Edge(line(0).toString.substring(1).toLong, line(1).toString.substring(1).toLong, "subclass"))

  //create graph
  lazy val graph = if (graphFromEdges) Graph.fromEdges(edgesRDD, ("No Label", 0)) else Graph(verticesRDD, edgesRDD, ("No Label", 0))
  
  //outDegree=0 => root class
  lazy val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter{case (_, neighbors) => neighbors.size==0}.leftJoin(verticesRDD){case (id, _, Some((label, _))) => (id, label); case (id, _, None) => (id, "No Label")}
  //inDegree=0 => leaf class
  lazy val leafClasses = graph.collectNeighborIds(EdgeDirection.In).filter{case (_, neighbors) => neighbors.size==0}.leftJoin(verticesRDD){case (id, _, Some((label, _))) => (id, label); case (id, _, None) => (id, "No Label")} 
  
  def firstLevelStatistics {
	  //vertices = classes, edges = subclass relations
	  println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges)

	  //graph from edges doesn't have labeled vertices => join with vertices RDD
	  if (graphFromEdges)
		  println("Classes without label: " + graph.vertices.join(verticesRDD).filter{ case (_, (_, (label, _))) => label.equals("No Label")}.count())
	  else
		  println("Classes without label: " + graph.vertices.filter{case (_, (label, _)) => label.equals("No Label")}.count())    
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
	  val subgraphs = graph.connectedComponents().vertices.join(verticesRDD).leftOuterJoin(rootClasses).map{ case ((id, ((graphId, (label, numOfInstances)), Some((_,isRoot))))) => (graphId, (id, label, numOfInstances, true)) ; case ((id, ((graphId, (label, numOfInstances)), None))) => (graphId, (id, label, numOfInstances, false))}.groupByKey.collect
    
	  //write metadata about subgraphs
    val metadata1 = new PrintWriter(new File("subgraphs.txt" ))
    metadata1.write("#Subgraphs: " + subgraphs.length + "\n")
    subgraphs.foreach{ case (graphId, vertices) => 
    metadata1.write("======Graph " + graphId + " (#Roots: " + vertices.count{case (_, _, _, isRoot) => isRoot} + ", #Classes: " + vertices.size + ")======\n")
    vertices.foreach{ case (_, label, _, isRoot) => if (isRoot) metadata1.write(label + "\n")}}
    metadata1.close()
    
    val metadata2 = new PrintWriter(new File("subgraphs.tsv" ))
    subgraphs.foreach{ case (graphId, vertices) => 
    metadata2.write("G" + graphId + separator + vertices.size + separator + vertices.map(f => (f._3)).sum + "\n")}
    metadata2.close()
  }

  def main(args: Array[String]) {

	  if (args.length == 2) {
		  verticesFile = args(0)
		  edgesFile = args(1)      
	   
		  //firstLevelStatistics
		  //hierarchyStatistics
		  createSubgraphs
	  }
  }
}