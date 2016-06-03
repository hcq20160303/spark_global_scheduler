package org.apache.spark.rddShare.globalScheduler

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.rddShare.globalScheduler.SchedulerMessages.{WaitOtherJobStarted, JobFinished, JobBegining, JobStart}
import org.apache.spark.rddShare.reuse.{Cacher, DAGMatcherAndRewriter, SimulateRDD}
import org.apache.spark.rddShare.tool.MyUtils
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
  private val indexOfDagScan = new util.ArrayList[Int]
  private val globalSchedulerEndpoint = rpcEnv.setupEndpointRef(SchedulerActor.SYSTEM_NAME, globalScheduler, SchedulerActor.ENDPOINT_NAME)

  override def onStart(): Unit ={
    // get the nodes and indexOfDagScan from dag
    DAGMatcherAndRewriter.transformDAGtoList(null, rdd, nodes, indexOfDagScan)
    // send message to Global Scheduler to scheduling
    globalSchedulerEndpoint.send(JobBegining(nodes, indexOfDagScan, self))
    logInfo("App.onStart: hello, I have successfully sent the beginning message to global scheduler.")

  }

  override def receive: PartialFunction[Any, Unit] = {

    case JobStart(rewrite: util.ArrayList[(Int, String)], cache: util.ArrayList[(Int, String)]) => {
      logInfo("---App.receive.JobStart: hello, I have received the start message from Global Scheduler")
      logInfo("---App.receive.JobStart: Start rewrite")
      logInfo("---receive rewrite: " + MyUtils.printArrayList(rewrite.toArray))
      // rewrite the dag corresponding the job
      DAGMatcherAndRewriter.rewriter(nodes, rewrite)
      logInfo("App.receive.JobStart: Start cache")
      logInfo("---receive cache: " + MyUtils.printArrayList(cache.toArray))
      // cache the rdds in this dag
      Cacher.cache(nodes, cache)
      // change the scheduling state to true, then the DAGScheduler can submit this job
      nodes.get(nodes.size()-1).realRDD.isSchedule = true
    }

    case JobFinished() => {
      globalSchedulerEndpoint.send(JobFinished())
      logInfo("---send job finished message to global scheduler")
    }

    case WaitOtherJobStarted() => {
      logInfo("---Wait 10s to resend job beginning message to global scheduler while there are other jobs " +
        "in global scheduler have not been started, so we need wait until all the other jobs have been started.")
      for ( i <- 0 to 10000 ){
        for ( j <- 0 to 10000 ){
          for ( k <- 0 to 10000 ){
            for ( l <- 0 to 10000 ){}
          }
        }
      }
      globalSchedulerEndpoint.send(JobBegining(nodes, indexOfDagScan, self))
      logInfo("---resend message to global scheduler.")
    }

    case _ => {
      logError("---App.receive._: I can't resolve this message. There's may be an error in Global Scheduler.")
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
