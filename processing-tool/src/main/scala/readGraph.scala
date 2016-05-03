import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
import scala.util.Try
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.collection.mutable.MutableList

object readGraph {

  var classesFile, subclassOfRelationsFile, instancesFile: String = null

  val separator = "\t"
  val newline = "\n"

  case class Vertex(val label: String, val instances: List[VertexId], val isRoot: Boolean)

  //initialize Spark
  val conf = new SparkConf().setAppName("readGraph").setMaster("local")
  val sc = new SparkContext(conf)

  //create graph from edges ignoring single node classes
  val graphFromEdges = false

  //remove classes without label
  val removeClassesWithoutLabel = true

  //reads instances file
  val readInstances = false

  //read from tsv files  
  lazy val verticesRDD = {
    lazy val classesRDD = sc.textFile(classesFile).map(line => line.split(separator)).map { case Array(id, label) => (id.toString.toLong, Vertex(label.toString, null, false)); case _ => (0L, Vertex("No label", null, false)) }
    lazy val instancesRDD = sc.textFile(instancesFile).map(line => line.split(separator)).map(line => (line(1).toString.toLong, line(0).toString.toLong))

    if (readInstances)
      classesRDD.leftOuterJoin(instancesRDD).map { case (id, (vertex, instances)) => ((id, vertex), instances.getOrElse(-1L)) }.groupByKey.map { case ((id, vertex), instances) => (id, Vertex(vertex.label, instances.filterNot { f => f == -1L }.toList, vertex.isRoot)) }
    else
      classesRDD
  }
  lazy val edgesRDD = sc.textFile(subclassOfRelationsFile).map(line => line.split(separator)).map(line => Edge(line(0).toString.toLong, line(1).toString.toLong, "subclassOf"))

  //create graph
  lazy val graph = {
    lazy val initialGraph = {
      if (graphFromEdges) Graph.fromEdges(edgesRDD, Vertex("No Label", List.empty, true)).joinVertices(verticesRDD) { case (_, _, vertex) => vertex }
      else Graph(verticesRDD, edgesRDD, Vertex("No Label", List.empty, true))
    }

    if (removeClassesWithoutLabel) initialGraph.subgraph(vpred = (id, vertex) => vertex.label != "No Label") else initialGraph
  }.cache()

  //outDegree=0 => root node
  lazy val rootNodes = graph.collectNeighborIds(EdgeDirection.Out).filter { case (_, neighbors) => neighbors.isEmpty }.join(graph.vertices).map { case (id, (_, vertex)) => (id, vertex) }
  //inDegree=0 => leaf node
  lazy val leafNodes = graph.collectNeighborIds(EdgeDirection.In).filter { case (_, neighbors) => neighbors.isEmpty }.join(graph.vertices).map { case (id, (_, vertex)) => (id, vertex) }

  //compute subgraphs
  lazy val subgraphs = graph.connectedComponents().vertices.join(verticesRDD).leftOuterJoin(rootNodes).map { case ((id, ((graphId, vertex), rootVertex))) => (graphId, (id, Vertex(vertex.label, vertex.instances, rootVertex.exists(_ => true)))) }.groupByKey

  def firstLevelStatistics {
    //vertices = classes, edges = subclass relations
    println("Classes: " + graph.numVertices, "Subclasses: " + graph.numEdges)

    println("Classes without label: " + graph.vertices.filter { case (_, vertex) => vertex.label.equals("No Label") }.count())
    println("Classes without instanses: " + graph.vertices.filter { case (_, vertex) => vertex.instances.size.equals(0) }.count())
  }

  def hierarchyStatistics {
    //root /\ leaf => single node
    val singleNodeClasses = rootNodes.intersection(leafNodes)
    //root - leaf => root but not single node
    val rootNotSingleNodeClasses = rootNodes.subtract(leafNodes)

    //print statistics
    println("Root Classes: " + rootNodes.count)
    println("Leaf Classes: " + leafNodes.count)
    println("Single Node Classes: " + singleNodeClasses.count)
    println("Root not Single Node Classes: " + rootNotSingleNodeClasses.count)
  }

  def subgraphsStatistics {

    //write metadata about subgraphs
    val metadata1 = new PrintWriter(new File("results/subgraphs.txt"))
    metadata1.write("#Subgraphs: " + subgraphs.count + newline)
    subgraphs.collect.foreach {
      case (graphId, vertices) =>
        metadata1.write("======Graph " + graphId + " (#Roots: " + vertices.count { case (_, vertex) => vertex.isRoot } + ", #Classes: " + vertices.size + ")======\n")
        vertices.foreach { case (_, vertex) => if (vertex.isRoot) metadata1.write(vertex.label + newline) }
    }
    metadata1.close()

    val metadata2 = new PrintWriter(new File("results/subgraphs.tsv"))
    subgraphs.collect.foreach {
      case (graphId, vertices) =>
        metadata2.write("G" + graphId + separator + vertices.size)
    }
    metadata2.close()
  }

  def extractSubgraph(id: Int) {
    val subgraph = Graph(subgraphs.filter { case (graphId, _) => graphId == id }.map { case (_, vertices) => vertices }.flatMap(f => f), edgesRDD, Vertex("No Label", null, true)).subgraph(vpred = (id, vertex) => vertex.label != "No Label")

    val writer1 = new PrintWriter(new File("results/Graph" + id + "_Edges.tsv"))
    subgraph.edges.collect.foreach { edge => writer1.write(edge.srcId + separator + edge.dstId + newline) }
    writer1.close()

    val writer2 = new PrintWriter(new File("results/Graph" + id + "_Vertices.tsv"))
    subgraph.vertices.collect.foreach { case (id, vertex) => writer2.write(id + separator + vertex.label + newline) }
    subgraph.vertices.collect.foreach { case (id, vertex) => writer2.write(id + newline) }
    writer2.close()

    val writer3 = new PrintWriter(new File("results/Graph" + id + "_Roots.tsv"))
    subgraph.vertices.filter { case (_, vertex) => vertex.isRoot }.collect.foreach { case (id, vertex) => writer3.write(id + separator + vertex.label + newline) }
    writer3.close()
  }

  def computeNumOfInstances {
    val writer = new PrintWriter(new File("results/numOfInstances.tsv"))
    var visited: Map[VertexId, Set[VertexId]] = Map.empty
    graph.collectNeighborIds(EdgeDirection.Out).filter { case (_, neighbors) => neighbors.isEmpty }.collect.foreach { case (id, _) => computeNumOfInstances(id, visited, writer) }
    writer.close()
  }

  def computeNumOfInstances(frontier: VertexId, visited: Map[VertexId, Set[VertexId]], writer: PrintWriter) {
    try {

      visited += (frontier -> Set.empty[VertexId])

      val subclasses = graph.edges.filter { edge => edge.dstId == frontier }.map { edge => edge.srcId }.collect.iterator

      var instances = Set.empty[VertexId]
      while (subclasses.hasNext) {
        val subclass = subclasses.next
        if (subclass != frontier) {
          if (!visited.contains(subclass))
            computeNumOfInstances(subclass, visited, writer)
          instances = instances ++ visited.getOrElse(subclass, Set.empty[VertexId])
        }
      }

      val (id, vertex) = graph.vertices.filter { case (id, _) => id == frontier }.first
      instances = instances ++ vertex.instances.toSet
      visited += (frontier -> instances)

      writer.write(id + separator + vertex.label + separator + instances.size + newline)

    } catch {
      case _: Throwable => writer.close()
    }
  }

  def extractRandomEdges(num: Int) {
    val writer = new PrintWriter(new File("results/"+num+"randomEdges.tsv"))
    writer.write("srcURL" + separator + "srcLabel" + separator + "relation" + separator + "dstURL" + separator + "dstLabel" + newline)

    for (i <- 1 to num) {
      val randomEdge = {
        var randomEdges: Array[Edge[String]] = null
        do {
          var randomVertex = graph.pickRandomVertex
          randomEdges = graph.edges.filter { edge => edge.srcId == randomVertex }.collect()
        } while (randomEdges.isEmpty)
        randomEdges(0)
      }

      val (srcId, srcVertex) = graph.vertices.filter { case (id, _) => id == randomEdge.srcId }.first
      val (dstId, dstVertex) = graph.vertices.filter { case (id, _) => id == randomEdge.dstId }.first
      writer.write("http://www.wikidata.org/wiki/Q" + srcId + separator + srcVertex.label + separator + randomEdge.attr + separator + "http://www.wikidata.org/wiki/Q" + dstId + separator + dstVertex.label + newline)
    }
    writer.close()
  }

  
  def computeAvgDepth {
     
    var paths: MutableList[Int] = MutableList.empty[Int]
    graph.collectNeighborIds(EdgeDirection.Out).filter { case (_, neighbors) => neighbors.isEmpty }.collect.foreach { case (id, _) => computeAvgDepth(id, Set.empty[VertexId], 0, paths) }
    
    println(paths.sum)
    println(paths.size)
  }
  
  def computeAvgDepth(frontier: VertexId, visited: Set[VertexId], depth: Int, paths: MutableList[Int]) {
    visited += frontier

      val subclasses = graph.edges.filter { edge => edge.dstId == frontier }.map { edge => edge.srcId }.collect.iterator

      if (!subclasses.hasNext) {
        paths += depth
      }
      
      while (subclasses.hasNext) {
        val subclass = subclasses.next
        if (!visited.contains(subclass)) {
            computeAvgDepth(subclass, visited, depth + 1, paths)
        }
      }
  }
  
  def main(args: Array[String]) {

    if (args.length == 3) {
      classesFile = args(0)
      subclassOfRelationsFile = args(1)
      instancesFile = args(2)

      //firstLevelStatistics
      //hierarchyStatistics
      //subgraphsStatistics
      //extractSubgraph(3)
      //computeNumOfInstances
      //extractRandomEdges(1000)
      computeAvgDepth
    }
  }
}