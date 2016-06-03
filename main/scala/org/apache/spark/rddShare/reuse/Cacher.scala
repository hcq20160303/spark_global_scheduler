package org.apache.spark.rddShare.reuse

import java.io._
import java.util
import java.util.Calendar
import java.util.function.Consumer

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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
  def cache(nodes:  util.ArrayList[SimulateRDD], cache:  util.ArrayList[(Int, String)]): Unit ={

    cache.forEach(new Consumer[(Int, String)] {
      override def accept(che: (Int, String)): Unit = {
        var skipRDD = nodes.get(che._1).realRDD
        if ( che._1 == (nodes.size() - 1)) {
          println("---Cacher.cache: skip rdd ready.")
          var stopSkip = false
          while (!stopSkip && skipRDD != null) {
            while (skipRDD.transformation.equals("default")) {
              println("---Cacher.cache: skip rdd " + skipRDD.id + ": " + skipRDD.transformation)
              skipRDD = skipRDD.dependencies(0).rdd
              stopSkip = true
            }
            if (!stopSkip) {
              if (skipRDD.dependencies != null) {
                skipRDD = skipRDD.dependencies(0).rdd
              } else {
                skipRDD = null
              }
            }
          }
        }
        var realRDD = nodes.get(che._1).realRDD
        if ( skipRDD != null ) {
           realRDD = skipRDD
        }
        realRDD.isCache = true
        val cachePath = che._2
        realRDD.cache()

        val begin = System.currentTimeMillis()
//        realRDD.sparkContext.setCheckpointDir(CacheManager.getRepositoryBasePath)
        realRDD.saveAsObjectFile(cachePath)
//        realRDD.saveAsTextFile(cachePath)
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

        val cacheNodes = new util.ArrayList[SimulateRDD](nodes.subList(realRDD.indexOfleafInNodesList, realRDD.indexOfnodesList+1))
        val indexOfDagScan = new util.ArrayList[Int]
        println("CacheNodes: ")
        cacheNodes.forEach(new Consumer[SimulateRDD] {
          override def accept(t: SimulateRDD): Unit = {
            println(t)
            if ( t.transformation.equalsIgnoreCase("textFile") || t.transformation.equalsIgnoreCase("objectFile")){
              indexOfDagScan.add(cacheNodes.indexOf(t))
            }
          }
        })
        val calendar = Calendar.getInstance()
        val now = calendar.getTime()
        val insertTime = new java.sql.Timestamp(now.getTime())
        val addCache = new CacheMetaData(0, cacheNodes, indexOfDagScan
          , cachePath, modifiedTime, fileSize, (end-begin), insertTime)
        CacheManager.checkCapacityEnoughElseReplace(addCache)
      }
    })
  }
}
