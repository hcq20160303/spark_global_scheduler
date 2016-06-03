package org.apache.spark.rddShare.globalScheduler

import java.util

import org.apache.spark.rddShare.reuse.SimulateRDD
import org.apache.spark.rpc.RpcEndpointRef

/**
 * Created by hcq on 16-5-23.
 */
object SchedulerMessages {

  // these messages are sent by apps
//  case class JobBegining(nodes: Array[SimulateRDD], indexOfDagScan: Array[Int], job: RpcEndpointRef)
  case class JobBegining(nodes: util.ArrayList[SimulateRDD], indexOfDagScan: util.ArrayList[Int], job: RpcEndpointRef)
  case class JobFinished()

  // these messages are sent by global scheduler
  case class JobStart(rewrite: util.ArrayList[(Int, String)], cache: util.ArrayList[(Int, String)])
  case class WaitOtherJobStarted()
}
