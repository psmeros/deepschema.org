import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

object readGraph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //WARNING: Requires 2 arguments
    val nodesFile = sc.textFile(args(0));
    val edgesFile = sc.textFile(args(1));
    
    val nodesRDD: RDD[(VertexId,String)] = nodesFile.map(line => line.split(",")).map(line => (line(0).toString.substring(1).toInt:VertexId, line(1).toString()))
    val edgesRDD: RDD[Edge[String]] = edgesFile.map(line => line.split(",")).map(line => Edge(line(0).toString.substring(1).toInt:VertexId, line(1).toString.substring(1).toInt:VertexId, "subclass"))
    
    val graphFromEdges = true
    
    val graph = if (graphFromEdges) Graph.fromEdges(edgesRDD, "No Label") else Graph(nodesRDD, edgesRDD, "No Label")
    
    //vertices = classes, edges = subclass relations
    println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges)
    
    if (graphFromEdges)
    	println("Classes without label: " + graph.vertices.map(f => (f._1, 0)).join(nodesRDD).filter(_._2._2.equals("No Label")).count())
    else
    	println("Classes without label: " + graph.vertices.filter(_._2.equals("No Label")).count())


    //outDegree=0 => root class
    val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter(f => f._2.size==0).map(f => (f._1, 0))
    //inDegree=0 => leaf class
    val leafClasses = graph.collectNeighborIds(EdgeDirection.In).filter(f => f._2.size==0).map(f => (f._1, 0)) 
    //outDegree=0 /\ inDegree=0 => single node
    val singleNodeClasses = rootClasses.intersection(leafClasses)
    //outDegree=0 - inDegree=0 => root but not single node
    val rootNotSingleNodeClasses = rootClasses.subtract(leafClasses)
    
    
    println("Root Classes: " + rootClasses.count())
    println("Leaf Classes: " + leafClasses.count())
    println("Single Node Classes: " + singleNodeClasses.count())    
    println("Root not Single Node Classes: " + rootNotSingleNodeClasses.count())
  
    //write classes URI and label         
    val writer1 = new PrintWriter(new File("singleNodeClasses.csv" ))
    val writer2 = new PrintWriter(new File("rootNotSingleNodeClasses.csv" ))
    singleNodeClasses.join(nodesRDD).map(f => (f._1, f._2._2)).collect.foreach(f => writer1.write("http://www.wikidata.org/wiki/Q" + f._1 + " , " + f._2 + "\n"))
    rootNotSingleNodeClasses.join(nodesRDD).map(f => (f._1, f._2._2)).collect.foreach(f => writer2.write("http://www.wikidata.org/wiki/Q" + f._1 + " , " + f._2 + "\n"))
    writer1.close()
    writer2.close()
        
    val subgraphs = graph.connectedComponents().vertices.join(nodesRDD).leftOuterJoin(rootClasses).map(f => (f._2._1._1, (f._1, f._2._1._2, f._2._2))).groupByKey.collect
          
    val metadata = new PrintWriter(new File("metadata.txt" ))
    subgraphs.foreach{ f => metadata.write("======Graph " + f._1 + "======\n") ; f._2.foreach{ f =>  if (f._3.isEmpty.unary_!) metadata.write(f._2 + "\n")}}
    metadata.close()
    
    }
}