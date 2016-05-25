package org.apache.spark.rddShare.test

import org.apache.spark.rpc.RpcEndpointRef

/**
 * Created by hcq on 16-5-23.
 */
object SchedulerMessages {

  // these messages are sent by apps
  case class JobBegining(nodes: String, job: RpcEndpointRef)
  case class JobFinished(job: RpcEndpointRef)

  // these messages are sent by global scheduler
  case class JobStart(rewrite: String)
}
