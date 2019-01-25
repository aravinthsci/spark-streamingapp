package com.streamapp.config

import com.typesafe.config.ConfigFactory

object Settings {

  private val config = ConfigFactory.load()

  object ProjectConfig {
    private val pro_Conf = config.getConfig("settings")
    lazy val checkPointDir: String = pro_Conf.getString("checkPoint")
  }

}
