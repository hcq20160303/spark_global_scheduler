package org.apache.spark.rddShare.globalScheduler

import org.apache.spark.rddShare.globalScheduler.SchedulerMessages.{JobStart, JobFinished, JobBegining}
import org.apache.spark.rddShare.reuse.SimulateRDD
import org.apache.spark.rpc._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hcq on 16-5-23.
 * this global scheduler runs on this method:
 * 1. first step, it collect the jobs submitted by user through the receive method
 * 2. after the number of jobs reach the APPS_NUMBER_IN_ONE_SCHEDULING, then scheduling
 *    these jobs, this scheduling calls ONE_SCHEDULING
 */
class SchedulerActor(
                      override val rpcEnv: RpcEnv,
                      val address: RpcAddress) extends ThreadSafeRpcEndpoint with Logging {
  // when the number of jobs in list has reach this threshold,
  // it will trigger the schedule process
  private val JOBS_NUMBER_IN_ONE_SCHEDULING=10
  private val jobsInOneScheduling = new ArrayBuffer[JobInformation]()
  private val performOrderOfJobs = new Array[Int](JOBS_NUMBER_IN_ONE_SCHEDULING)

  override def receive: PartialFunction[Any, Unit] = {

    case JobBegining(nodes: Array[SimulateRDD], indexOfDagScan: Array[Int], job: RpcEndpointRef) => {
      println("SchedulerActor.receive: I have got the message from " + nodes)
      val jobInfor = new JobInformation(nodes, indexOfDagScan, job)
      jobsInOneScheduling += jobInfor
      if ( jobsInOneScheduling.length == JOBS_NUMBER_IN_ONE_SCHEDULING ){
        scheduling()
        jobsInOneScheduling.clear()
      }
    }

    case JobFinished( job: RpcEndpointRef) => {
      // if a job finished, then send a start-job message to the apps
      // which need to reuse the result of this job
      jobsInOneScheduling.find(p => p.job.equals(job)) match {
        case Some(job: JobInformation) => {
          job.jobsDependentThisJob.foreach( jobInfo => jobInfo.job.send(JobStart(jobInfo.rewrite.toArray[(Int, String)])))
        }
        case _ => {
          println("SchedulerActor.receive.JobFinished: I can't find the finished job in my received jobs")
        }
      }
    }

    case _ => {
      println("SchedulerActor.receive._: I can't resolve this message.")
    }
  }

  // this method generate the suitable perform order of apps through the GA algorithm
  private def scheduling(): Unit ={

  }

}

class JobInformation(val nodes: Array[SimulateRDD],
                     val indexOfDagScan: Array[Int],
                     val job: RpcEndpointRef){

  val jobsDependentThisJob = new ArrayBuffer[JobInformation]()
  val rewrite = new ArrayBuffer[(Int, String)]

}

object SchedulerActor{

  val SYSTEM_NAME="rddShareGLOBALSCHEDULER"
  val ENDPOINT_NAME="GLOBALSCHEDULER"
  val PORT=34110

  def main(argStrings: Array[String]) {
    val conf = new SparkConf
    val rpcEnv = startRpcEnvAndEndpoint(Utils.localHostName(), PORT, conf)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              conf: SparkConf): RpcEnv = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new SchedulerActor(rpcEnv, rpcEnv.address))
    rpcEnv
  }

}
