package org.apache.spark.rddShare.globalScheduler

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.rddShare.globalScheduler.SchedulerMessages.{JobBegining, JobFinished, JobStart}
import org.apache.spark.rddShare.reuse.{CacheManager, DAGMatcherAndRewriter, SimulateRDD}
import org.apache.spark.rpc._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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
  // the scheduling order
  private val schedulingOrder = new Array[Int](SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING)
  // the first start jobs which have no dependencies after scheduling
  private val jobConcurrentStarted = new ArrayBuffer[Int]()
  // get the started jobs after scheduling
  private var jobStarted = 0
  // set the GA algorithm's parameters
  private val ga = new GA(SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING)

  override def receive: PartialFunction[Any, Unit] = {

    case JobBegining(nodes: Array[SimulateRDD], indexOfDagScan: Array[Int], job: RpcEndpointRef) => {
      logInfo("SchedulerActor.receive: I have got the message from " + job.name)
      logInfo("nodes: "+nodes.foreach(x => x.toString()))
      logInfo("indexOfDagScan: "+indexOfDagScan.foreach(x => x.toString()))
      val jobInfor = new JobInformation(nodes, indexOfDagScan, job)
      jobsInOneScheduling += jobInfor
      if ( jobsInOneScheduling.length == SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING ){
        schedulingBasedGA()
        logInfo("finished scheduling. Now I must launch the first job")
        // start the first job
        val firstJob = jobsInOneScheduling(schedulingOrder(0))
        firstJob.job.send(JobStart(firstJob.rewrite.toArray, firstJob.cache.toArray))
        jobStarted += 1
        logInfo("launch Job finished. Does app has received my message?")
//        jobConcurrentStarted.foreach(index => {
//          jobStarted += 1
//          val jobStart = jobsInOneScheduling(index)
//          // send message to the job so that it can start its job
//          jobStart.job.send(JobStart(jobStart.rewrite.toArray, jobStart.cache.toArray))
//        })
        while ( jobStarted < SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING ){
          // wait for all the jobs started in this one scheduling
        }
        jobsInOneScheduling.clear()
        jobStarted = 0
      }
    }

    case JobFinished() => {
      // if a job finished, then send a start-job message to the next job
      jobsInOneScheduling(schedulingOrder(jobStarted))
      jobStarted += 1
//      jobsInOneScheduling.find(p => p.job.equals(job)) match {
//        case Some(job: JobInformation) => {
//          job.jobsDependentThisJob.foreach( jobInfo => {
//            jobStarted += 1
//            jobInfo.job.send(JobStart(jobInfo.rewrite.toArray, jobInfo.cache.toArray))
//          })
//        }
//        case None => {
//          logError("SchedulerActor.receive.JobFinished: I can't find the finished job in my received jobs")
//        }
//      }
    }

    case _ => {
      logError("SchedulerActor.receive._: I can't resolve this message.")
    }
  }

  // this method generate the suitable perform order of apps through the GA algorithm
  private def schedulingBasedGA(): Unit ={
    ga.computeReuseMatrix(jobsInOneScheduling.toArray)
//    ga.iteration.copyToArray(schedulingOrder)
    val jobNum = SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING
    val indexArray = Array.range(0, jobNum)
    var end = jobNum -1
    for ( j <- 0 to jobNum-1){
      val index = Random.nextInt(end + 1)
      schedulingOrder(j) = indexArray(index)
      indexArray(index) = indexArray(end)
      end -= 1
    }
    logInfo("---schedulingOrder: " + schedulingOrder.mkString(", "))
//    getJobConcurrentStarted
    // get the jobConcurrentStarted and rewriting and caching information from schedulingOrder
    getRewriteandCacheInfoFromSchedulingOrder
  }

//  // find the jobs that can concurrent starting in first time
//  private def getJobConcurrentStarted: Unit ={
//    val jobsNum = SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING
//    jobConcurrentStarted += schedulingOrder(0)
//    val reuse = ga.reuse
//    var findJobConcurrentFinished = false
//    var i = 1
//    while ( i < jobsNum-1 && !findJobConcurrentFinished ){
//      val late = schedulingOrder(i)
//      var j = 0
//      while ( j < i-1){
//        val pre = schedulingOrder(j)
//        if ( reuse(late)(pre) > .0){
//          findJobConcurrentFinished = true
//        }
//        j += 1
//      }
//      if ( !findJobConcurrentFinished){
//        jobConcurrentStarted += late
//      }
//      i += 1
//    }
//  }

  // get the rewrite and cache information between jobs from scheduling order
  private def getRewriteandCacheInfoFromSchedulingOrder: Unit ={
    val jobsNum = SchedulerActor.JOBS_NUMBER_IN_ONE_SCHEDULING
    val reuse = ga.reuse
    for ( i <- 1 to jobsNum-1 ){
      val late = schedulingOrder(i)
      var maxReuse = .0
      var maxReuseIndex = -1
      // find the job that has the max reusing with job i
      for ( j <- 0 to i-1 ){
        val pre = schedulingOrder(j)
        if ( reuse(late)(pre) > maxReuse ){
          maxReuse = reuse(late)(pre)
          maxReuseIndex = j
        }
      }
      // has find the max reusing job
      if ( maxReuseIndex != -1 ){
        logInfo("---find reusing job")
        // get the cache nodes of job maxReuseIndex and the rewrite nodes of job late
        val reusingJob = jobsInOneScheduling(late)
        val reusedJob = jobsInOneScheduling(maxReuseIndex)
        val (rewrite, cache) = DAGMatcherAndRewriter.matchTwoDags(reusingJob, reusedJob)
        val cachePath = CacheManager.getRepositoryBasePath + reusedJob.job.name + reusedJob.nodes(cache._2).transformation
        val cacheInfo = (cache._2, cachePath)
        reusedJob.cache += cacheInfo
        val rewriteInfo = (rewrite._2, cachePath)
        reusingJob.rewrite += rewriteInfo
      }
    }
  }
}

class JobInformation(val nodes: Array[SimulateRDD],
                     val indexOfDagScan: Array[Int],
                     val job: RpcEndpointRef){

//  // jobs which reusing the cache of this job
//  val jobsDependentThisJob = new ArrayBuffer[JobInformation]()
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

  val confPath = sys.env.getOrElse("SPARK_HOME", "/home/hcq/Desktop/spark_1.6.0")
  println(confPath)
  val conf = ConfigFactory.parseFile(new File(confPath + "/conf/rddShare/default.conf"))
  // when the number of jobs in list has reach this threshold,
  // it will trigger the schedule process
  val JOBS_NUMBER_IN_ONE_SCHEDULING: Int = conf.getInt("rddShare.jobsNumberinOneScheduling")

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