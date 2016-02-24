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
    
    val graph = Graph(nodesRDD, edgesRDD, "defaultLabel")    
    //val graph = Graph.fromEdges(edgesRDD, "defaultLabel")
    
    //vertices = classes, edges = subclass relations
    println("Classes: " + graph.numVertices, "Subclasses: " +  graph.numEdges)
    
    //vertices with default label = classes that don't have an english label
    println("Deleted or without english label classes: " + graph.vertices.filter(_._2.equals("defaultLabel")).count())

    //outDegree=0 => root class
    val rootClasses = graph.collectNeighborIds(EdgeDirection.Out).filter(f => f._2.size==0)
    
    println("Root Classes: " + rootClasses.count())
  
    //write root classes URI and label         
    val writer = new PrintWriter(new File("roots.csv" ))
    rootClasses.innerJoin(graph.vertices)((id, array, label) => label).collect().foreach(f => writer.write("http://www.wikidata.org/wiki/Q" + f._1 + " , " + f._2 + "\n"))
    writer.close()
    
    }
}