package org.apache.spark.rddShare.reuse

import java.io._
import java.util.Calendar

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hcq on 16-5-9.
 */
object Cacher {

  /**
   * this method save the rdd that needed to be cached,
   * and store the metaData of this cache into database
   * @param nodes: the nodes of dag
   * @param cache: the index and cache path of nodes which needed to be cached
   */
  def cache(nodes: Array[SimulateRDD], cache: Array[(Int, String)]): Unit ={

    cache.foreach( che => {
      val realRDD = nodes(che._1).realRDD
      realRDD.isCache = true
      val cachePath = che._2
      realRDD.cache()

      val begin = System.currentTimeMillis()
      realRDD.saveAsObjectFile(cachePath)
      val end = System.currentTimeMillis()

      // get the information of this metaData in this caching
      var fileSize = .0
      if ( CacheManager.getRepositoryBasePath.contains("hdfs")){   // use hdfs to cache the data
        val config = new Configuration()
        val path = new Path(cachePath)
        val hdfs = path.getFileSystem(config)
        val cSummary = hdfs.getContentSummary(path)
        fileSize = cSummary.getLength().toDouble/math.pow(1024, 3)
      }else{                                                       // use the local file to cache the data
        fileSize = FileUtils.sizeOfDirectoryAsBigInteger(new File(cachePath)).doubleValue()/math.pow(1024, 3)
      }

      val modifiedTime = CacheManager.getLastModifiedTimeOfFile(cachePath)

      val cacheNodes = nodes.slice(realRDD.indexOfleafInNodesList, realRDD.indexOfnodesList+1)
      val indexOfDagScan = new ArrayBuffer[Int]
      println("CacheNodes: ")
      cacheNodes.foreach( t => {
        println(t)
        if ( t.transformation.equalsIgnoreCase("textFile") || t.transformation.equalsIgnoreCase("objectFile")){
          indexOfDagScan += cacheNodes.indexOf(t)
        }
      })
      val calendar = Calendar.getInstance()
      val now = calendar.getTime()
      val insertTime = new java.sql.Timestamp(now.getTime())
      val addCache = new CacheMetaData(0, cacheNodes, indexOfDagScan.toArray
        , cachePath, modifiedTime, fileSize, (end-begin), insertTime)
      CacheManager.checkCapacityEnoughElseReplace(addCache)

    })
  }
}
