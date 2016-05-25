package org.apache.spark.rddShare.test

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.util.Utils
/**
 * Created by hcq on 16-5-23.
 */
object appActor2 {

  private val SYSTEM_NAME="APP2"

  def main(argStrings: Array[String]) {
    val conf = new SparkConf
    val rpcEnv = App.startRpcEnvAndEndpoint(Utils.localHostName(), 34, conf, SYSTEM_NAME)
    val appEndpoint = rpcEnv.setupEndpoint(App.ENDPOINT_NAME,
      new App(rpcEnv, SYSTEM_NAME, rpcEnv.address, RpcAddress.fromSparkURL("spark://172.26.30.250:"+SchedulerActor.PORT)))
    //    appEndpoint
    rpcEnv.awaitTermination()
  }
}
