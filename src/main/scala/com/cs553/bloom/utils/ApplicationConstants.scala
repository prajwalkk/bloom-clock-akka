package com.cs553.bloom.utils

import com.typesafe.config.ConfigFactory

/*
*
* Created by: prajw
* Date: 12-Nov-20
*
*/
object ApplicationConstants {


  val RULE_1 = 1
  val RULE_2 = 2
  val INTERNAL_EVENT = 1
  val SEND_EVENT = 2
  val RECV_EVENT = 3
  val SEED_1 = 42
  val SEED_2 = 211
  val SEED_3 = 120
  val SEED_4 = 25

  val RUN_NAME: String = ConfigFactory.load("constants.conf").getString("run")


}
