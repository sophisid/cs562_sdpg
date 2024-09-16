// Define the Node class
case class Node(
  label: String, 
  properties: Map[String, Any], 
  isOptional: Boolean = false, 
  minCardinality: Int = 1, 
  maxCardinality: Int = 1
)

// Define the Edge class
case class Edge(
  startNode: Node, 
  relationshipType: String, 
  endNode: Node, 
  properties: Map[String, Any], 
  isOptional: Boolean = false, 
  minCardinality: Int = 1, 
  maxCardinality: Int = 1
)

// Define the Constraint class
case class Constraint(
  field: String, 
  operation: String, 
  value: Any
)

// Define the Pattern class that holds nodes, edges, and constraints
class Pattern(
  var nodes: List[Node] = List(),
  var edges: List[Edge] = List(),
  var constraints: List[Constraint] = List()
) {

  // Add a node to the pattern
  def addNode(node: Node): Unit = {
    nodes = nodes :+ node
  }

  // Add an edge to the pattern
  def addEdge(edge: Edge): Unit = {
    edges = edges :+ edge
  }

  // Add a constraint to the pattern
  def addConstraint(constraint: Constraint): Unit = {
    constraints = constraints :+ constraint
  }

  // Display the pattern including nodes, edges, and constraints
  override def toString: String = {
    val nodeStr = nodes.map { node =>
      s"Node(label=${node.label}, optional=${node.isOptional}, cardinality=${node.minCardinality}..${if (node.maxCardinality == -1) "N" else node.maxCardinality})"
    }.mkString(", ")

    val edgeStr = edges.map { edge =>
      s"Edge(relationshipType=${edge.relationshipType}, optional=${edge.isOptional}, cardinality=${edge.minCardinality}..${if (edge.maxCardinality == -1) "N" else edge.maxCardinality})"
    }.mkString(", ")

    s"Nodes: [$nodeStr]\nEdges: [$edgeStr]\nConstraints: ${constraints.mkString(", ")}"
  }
}

// Example usage
// object Pattern {
//   def main(args: Array[String]): Unit = {
//     // Define some nodes
//     val alice = Node("Person", Map("name" -> "Alice", "age" -> 30), isOptional = false, minCardinality = 1, maxCardinality = 1)
//     val bob = Node("Person", Map("name" -> "Bob", "age" -> 35), isOptional = false, minCardinality = 1, maxCardinality = 1)
//     val company = Node("Company", Map("name" -> "TechCorp"), isOptional = true, minCardinality = 1, maxCardinality = -1)  // Optional node

//     // Define an edge
//     val worksAt = Edge(alice, "WORKS_AT", company, Map("since" -> "2020"), isOptional = false, minCardinality = 1, maxCardinality = 1)

//     // Define a constraint (e.g., find people whose age is greater than 30)
//     val ageConstraint = Constraint("age", ">", 30)

//     // Create the pattern
//     val pattern = new Pattern()

//     pattern.addNode(alice)
//     pattern.addNode(bob)
//     pattern.addNode(company)
//     pattern.addEdge(worksAt)
//     pattern.addConstraint(ageConstraint)

//     // Display the pattern
//     println(pattern.toString)
//   }
// }
