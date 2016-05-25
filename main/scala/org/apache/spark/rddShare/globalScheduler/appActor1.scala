
package org.apache.spark.rddShare.globalScheduler

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Created by hcq on 16-5-23.
 */

object appActor1{

  private val SYSTEM_NAME="APP1"

  def main(argStrings: Array[String]) {
    val rewrite = "hello"
    val conf = new SparkConf
    val rpcEnv = App.startRpcEnv(Utils.localHostName(), 0, conf, SYSTEM_NAME)
//    val appEndpoint = rpcEnv.setupEndpoint(App.ENDPOINT_NAME,
//      new App(rpcEnv, SYSTEM_NAME, rpcEnv.address, RpcAddress.fromSparkURL("spark://192.168.1.105:" + SchedulerActor.PORT), rewrite))
//    appEndpoint
    rpcEnv.awaitTermination()
  }

}