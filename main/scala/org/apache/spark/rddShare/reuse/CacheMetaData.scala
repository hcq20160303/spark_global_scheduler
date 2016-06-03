package org.apache.spark.rddShare.reuse

import java.sql.Timestamp
import java.util

import org.apache.spark.rddShare.tool.MyUtils

/**
 * Created by hcq on 16-5-5.
 * This class use to save the meta data of a cache
 */
class CacheMetaData(
      val id: Int,                                  // the auto-inc key in mysql table
      val nodesList: util.ArrayList[SimulateRDD],            // DAG图的各个节点
      val indexOfDagScan: util.ArrayList[Int],               // the leaf nodes(read file) of this DAG
      val outputFilename: String,                   // 结果保存的文件名
      val outputFileLastModifiedTime: Long,         // use to maintain consistency
      val sizeOfOutputData: Double,
      val exeTimeOfDag: Long,
      val insertTime: Timestamp
     ) extends Serializable {

  val root = nodesList.get(nodesList.size()-1)  // DAG的根节点
  var reuse: Int = 0

  override def toString: String ={
    "nodesList: " + MyUtils.printArrayList(nodesList.toArray) + "\tindexOfDagScan: "+indexOfDagScan.toString +
    "outputFilename: " + outputFilename + "\toutputFileLastModifiedTime: " + outputFileLastModifiedTime +
    "sizoOfOutputData: " + sizeOfOutputData + "\texeTimeOfDag: " + exeTimeOfDag
  }
}
