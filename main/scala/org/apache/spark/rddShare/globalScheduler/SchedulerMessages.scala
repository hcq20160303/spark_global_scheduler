package org.apache.spark.rddShare.globalScheduler

import org.apache.spark.rddShare.reuse.core.SimulateRDD
import org.apache.spark.rpc.RpcEndpointRef

/**
 * Created by hcq on 16-5-23.
 */
object SchedulerMessages {

  // these messages are sent by apps
  case class JobBegining(nodes: Array[SimulateRDD], job: RpcEndpointRef)
  case class JobFinished(job: RpcEndpointRef)

  // these messages are sent by global scheduler
  case class JobStart(rewrite: Array[(Int, String)])
}
