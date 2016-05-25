package org.apache.spark.rddShare.test

import org.apache.spark.rddShare.test.SchedulerMessages.{JobBegining, JobStart}
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.rpc._
import scala.concurrent.duration._
/**
 * Created by hcq on 16-5-23.
 */
class App(override val rpcEnv: RpcEnv,
          val systemName: String,
          val address: RpcAddress,
          val globalScheduler: RpcAddress) extends ThreadSafeRpcEndpoint with Logging {

  override def onStart(): Unit ={

    val globalSchedulerEndpoint = rpcEnv.setupEndpointRef(SchedulerActor.SYSTEM_NAME, globalScheduler, SchedulerActor.ENDPOINT_NAME)
    val jobStart = globalSchedulerEndpoint.ask(JobBegining("app1", self), new RpcTimeout(1 days, "spark.rpc.long.timeout"))

    println(jobStart.asInstanceOf[JobStart].rewrite)

  }

}

object App{

  val ENDPOINT_NAME="APP"

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              conf: SparkConf,
                              systemName: String): RpcEnv = {
    val securityMgr = new SecurityManager(conf)
    val config = RpcEnvConfig(conf, systemName, host, port, securityMgr, false)
    val rpcEnvFactory = Utils.classForName("org.apache.spark.rpc.netty.NettyRpcEnvFactory").newInstance().asInstanceOf[RpcEnvFactory]
    val rpcEnv = rpcEnvFactory.create(config)
    rpcEnv
  }
}
