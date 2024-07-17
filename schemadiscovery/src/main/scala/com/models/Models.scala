package com.models

@SerialVersionUID(1L)
case class Pattern(nodeLabel: String, properties: Map[String, Boolean], edges: Set[(String, String)]) extends Serializable
