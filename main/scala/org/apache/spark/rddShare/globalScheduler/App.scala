package org.apache.spark.rddShare.globalScheduler

import org.apache.spark.rddShare.globalScheduler.SchedulerMessages.{JobStart, JobBegining}
import org.apache.spark.rddShare.reuse.core.SimulateRDD
import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hcq on 16-5-23.
 */
class App(override val rpcEnv: RpcEnv,
          val systemName: String,
          val address: RpcAddress,
          val globalScheduler: RpcAddress,
          val nodes: Array[SimulateRDD], 
          val rewrite: ArrayBuffer[(Int, String)] ) extends ThreadSafeRpcEndpoint with Logging {

  override def onStart(): Unit ={

    val globalSchedulerEndpoint = rpcEnv.setupEndpointRef(SchedulerActor.SYSTEM_NAME, globalScheduler, SchedulerActor.ENDPOINT_NAME)
    globalSchedulerEndpoint.send(JobBegining(nodes, self))
    println("App.onStart: I have sent the beginning message to global scheduler.")

  }

  override def receive: PartialFunction[Any, Unit] = {

    case JobStart(rewrite: Array[(Int, String)]) => {
      rewrite.foreach(t => this.rewrite += t)
    }
  }

}

object App{

  val ENDPOINT_NAME="APP"
  
  def startRpcEnv(
                              host: String,
                              port: Int,
                              conf: SparkConf,
                              systemName: String): RpcEnv = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    rpcEnv
  }
}
