package org.apache.spark.rddShare.test

import org.apache.spark.rddShare.globalScheduler.GA

/**
 * Created by hcq on 16-5-29.
 */
class testGA {

}

object testGA{

  def main(args: Array[String]): Unit = {
    val ga = new GA(3)
    ga.initReuseMatrix
    ga.iteration
  }
}
