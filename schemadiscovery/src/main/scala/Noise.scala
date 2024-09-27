import scala.util.Random

object Noise {

  // Introduce noise to a list of nodes represented as a list of maps
  def addNoise(nodes: List[Map[String, Any]], noisePercentage: Double): List[Map[String, Any]] = {
    val random = new scala.util.Random

    // For each node, randomly nullify some properties based on noisePercentage
    nodes.map { node =>
      val numProperties = node.size
      val numToNullify = (numProperties * noisePercentage).toInt

      // Randomly select properties to nullify
      val keysToNullify = Random.shuffle(node.keys.toList).take(numToNullify)

      // Create a new map with the selected properties set to null
      node.map {
        case (key, value) if keysToNullify.contains(key) => (key, null)
        case other => other
      }
    }
  }
}
