package org.apache.spark.rddShare.globalScheduler

import java.util
import java.util.function.Consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.rddShare.globalScheduler.SchedulerMessages.{JobBegining, JobStart}
import org.apache.spark.rddShare.reuse.{DAGMatcherAndRewriter, SimulateRDD}
import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf}

/**
 * Created by hcq on 16-5-23.
 */
class App(override val rpcEnv: RpcEnv,
          val systemName: String,
          val address: RpcAddress,
          val globalScheduler: RpcAddress,
          val rdd: RDD[_]
           ) extends ThreadSafeRpcEndpoint with Logging {

  private val nodes = new util.ArrayList[SimulateRDD]
  private val indexOfDagScan = new util.ArrayList[Integer]

  override def onStart(): Unit ={
    // get the nodes and indexOfDagScan from dag
    DAGMatcherAndRewriter.transformDAGtoList(null, rdd, nodes, indexOfDagScan)
    println("nodes: "+nodes.forEach(new Consumer[SimulateRDD] {
      override def accept(t: SimulateRDD): Unit = {
        println(t.toString())
      }
    }))
    println("indexOfDagScan: "+indexOfDagScan.forEach(new Consumer[Integer] {
      override def accept(t: Integer): Unit = {
        println(t.toString())
      }
    }))
    // send message to Global Scheduler to scheduling
    val globalSchedulerEndpoint = rpcEnv.setupEndpointRef(SchedulerActor.SYSTEM_NAME, globalScheduler, SchedulerActor.ENDPOINT_NAME)
    globalSchedulerEndpoint.send(JobBegining(nodes, indexOfDagScan, self))
    logInfo("App.onStart: hello, I have successfully sent the beginning message to global scheduler.")

  }

  override def receive: PartialFunction[Any, Unit] = {

    case JobStart(rewrite: Array[(Int, String)], cache: Array[(Int, String)]) => {
      logInfo("App.receive.JobStart: hello, I have received the start message from Global Scheduler")
      logInfo("App.receive.JobStart: Start rewrite")
      // rewrite the dag corresponding the job
//      DAGMatcherAndRewriter.rewriter(nodes.toArray, rewrite)
//      logInfo("App.receive.JobStart: Start cache")
//      // cache the rdds in this dag
//      Cacher.cache(nodes.toArray, cache)
      // change the scheduling state to true, then the DAGScheduler can submit this job
      nodes.get(nodes.size()-1).realRDD.isSchedule = true
    }

    case _ => {
      logError("App.receive._: I can't resolve this message. There's may be an error in Global Scheduler.")
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
