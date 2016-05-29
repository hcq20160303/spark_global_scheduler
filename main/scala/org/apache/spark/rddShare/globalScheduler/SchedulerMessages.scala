package org.apache.spark.rddShare.globalScheduler

import org.apache.spark.rddShare.reuse.SimulateRDD
import org.apache.spark.rpc.RpcEndpointRef

/**
 * Created by hcq on 16-5-23.
 */
object SchedulerMessages {

  // these messages are sent by apps
  case class JobBegining(nodes: Array[SimulateRDD], indexOfDagScan: Array[Int], job: RpcEndpointRef)
  case class JobFinished()

  // these messages are sent by global scheduler
  case class JobStart(rewrite: Array[(Int, String)], cache: Array[(Int, String)])
}
