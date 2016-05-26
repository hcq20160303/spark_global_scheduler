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
  private val jobsInOneScheduling = new ArrayBuffer[JobInformation]()
  // the first start jobs which have no dependencies after scheduling
  private val jobConcurrentStarted = new ArrayBuffer[Int]()
  // get the started jobs after scheduling
  private var jobStarted = 0

  override def receive: PartialFunction[Any, Unit] = {

    case JobBegining(nodes: Array[SimulateRDD], indexOfDagScan: Array[Int], job: RpcEndpointRef) => {
      logInfo("SchedulerActor.receive: I have got the message from " + job.name)
      val jobInfor = new JobInformation(nodes, indexOfDagScan, job)
      jobsInOneScheduling += jobInfor
      if ( jobsInOneScheduling.length == SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING ){
        schedulingBasedGA()
        // start the first jobs
        jobConcurrentStarted.foreach(index => {
          jobStarted += 1
          val jobStart = jobsInOneScheduling(index)
          // send message to the job so that it can start its job
          jobStart.job.send(JobStart(jobStart.rewrite.toArray, jobStart.cache.toArray))
        })
        while ( jobStarted < SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING ){
          // wait for all the jobs started in this one scheduling
        }
        jobsInOneScheduling.clear()
        jobStarted = 0
      }
    }

    case JobFinished( job: RpcEndpointRef) => {
      // if a job finished, then send a start-job message to the apps
      // which need to reuse the result of this job
      jobsInOneScheduling.find(p => p.job.equals(job)) match {
        case Some(job: JobInformation) => {
          job.jobsDependentThisJob.foreach( jobInfo => {
            jobStarted += 1
            jobInfo.job.send(JobStart(jobInfo.rewrite.toArray, jobInfo.cache.toArray))
          })
        }
        case None => {
          logError("SchedulerActor.receive.JobFinished: I can't find the finished job in my received jobs")
        }
      }
    }

    case _ => {
      logError("SchedulerActor.receive._: I can't resolve this message.")
    }
  }

  // this method generate the suitable perform order of apps through the GA algorithm
  private def schedulingBasedGA(): Unit ={
  }

}

class JobInformation(val nodes: Array[SimulateRDD],
                     val indexOfDagScan: Array[Int],
                     val job: RpcEndpointRef){

  // jobs which reusing the cache of this job
  val jobsDependentThisJob = new ArrayBuffer[JobInformation]()
  // get the index of rdds which need to reusing the cache from other jobs in nodes,
  // and the path of reusing cache also need to store
  val rewrite = new ArrayBuffer[(Int, String)]
  // get the index of rdds which need to caching in nodes, and the cache path
  val cache = new ArrayBuffer[(Int, String)]

}

object SchedulerActor{

  val SYSTEM_NAME="rddShareGLOBALSCHEDULER"
  val ENDPOINT_NAME="GLOBALSCHEDULER"
  val PORT=34110

  // when the number of jobs in list has reach this threshold,
  // it will trigger the schedule process
  val JOBS_NUMBER_IN_ONE_SCHEDULING = 10

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
