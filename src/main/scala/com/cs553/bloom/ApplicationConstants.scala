package com.cs553.bloom

import com.typesafe.config.{Config, ConfigFactory}

/*
*
* Created by: prajw
* Date: 12-Nov-20
*
*/
object ApplicationConstants {

  val config: Config = ConfigFactory.load("constants.conf")
  val fileName: String = config.getString("FileName")
  val N: Int = config.getInt("numProcesses")
  val RULE_1 = 1
  val RULE_2 = 2
  val INTERNAL_EVENT = 1
  val SEND_EVENT = 2
  val RECV_EVENT = 3
  val SEED_1 = 42
  val SEED_2 = 211
  val SEED_3 = 120
  val SEED_4 = 25
  val K: Int = config.getInt("k")
  val BLOOM_CLOCK_LENGTH_RATIO: Double = config.getDouble("m")


}
