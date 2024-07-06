// Models.scala
package com.models

case class Pattern(nodeLabel: String, properties: Set[String], edges: Set[(String, String)])
