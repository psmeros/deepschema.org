import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object readGraph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //WARNING: Requires 2 arguments
    val nodesFile = sc.textFile(args(0));
    val edgesFile = sc.textFile(args(1));
    
    val nodesRDD: RDD[(VertexId,String)] = nodesFile.map(line => line.split(",")).map(line => (line(0).toString.substring(1).toInt:VertexId, line(1).toString()))
    val edgesRDD: RDD[Edge[String]] = edgesFile.map(line => line.split(",")).map(line => Edge(line(0).toString.substring(1).toInt:VertexId, line(1).toString.substring(1).toInt:VertexId, "subclass"))
    
    val graph = Graph(nodesRDD, edgesRDD)
    
    graph.triplets.collect.foreach(println)
    //println(graph.numEdges, graph.numVertices)
  }
}