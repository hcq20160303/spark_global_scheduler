package org.apache.spark.rddShare.reuse

import java.io._
import java.util
import java.util.{Calendar, ArrayList}

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.Properties

/**
 * Created by hcq on 16-5-9.
 */
object Cacher {

  val confPath = Properties.envOrElse("SPARK_HOME", "/home/hcq/Desktop/spark_1.5.0")
  val conf = ConfigFactory.parseFile(new File(confPath + "/conf/rddShare/default.conf"))

  // a RDD which execute a transformation in CACHE_TRANSFORMATION will be chosen to
  // store in repostory, and reuse by other application
  private val CACHE_TRANSFORMATION: Predef.Set[String] =
    conf.getString("rddShare.cacheTransformation").split(" ").toSet

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

  def getCacheRDD(nodesList: ArrayList[SimulateRDD]): Unit = {
    val size = nodesList.size()
    println("Cacher.scala---nodesList.toString: " + nodesList.toString)
    var i = size - 1
    while ( i > -1){
      val node = nodesList.get(i)
      // cache this RDD if this RDD is contained by the CACHE_TRANSFORMATION
      println("Cacher.scala---node" + i +" transformation: " + node.transformation)
      if ( CACHE_TRANSFORMATION.contains(node.transformation) ){
        node.realRDD.isCache = true

        val cachePath = CacheManager.getRepositoryBasePath + node.realRDD.sparkContext.hashCode() + System.currentTimeMillis().toString +
          node.realRDD.transformation
        node.realRDD.cache()

        val begin = System.currentTimeMillis()
        node.realRDD.saveAsObjectFile(cachePath)
        val end = System.currentTimeMillis()

        var fileSize = .0
        if ( CacheManager.getRepositoryBasePath.contains("hdfs")){   // use hdfs to cache the data
        val config = new Configuration()
          val path = new Path(cachePath)
          val hdfs = path.getFileSystem(config)
          val cSummary = hdfs.getContentSummary(path)
          fileSize = cSummary.getLength().toDouble/math.pow(1024, 3)
        }else{                                                       // use the local file to cache the data
          println("Cache.scala---getCacheRDD")
          fileSize = FileUtils.sizeOfDirectoryAsBigInteger(new File(cachePath)).doubleValue()/math.pow(1024, 3)
          println("fileSize: " + fileSize)
        }

        val modifiedTime = CacheManager.getLastModifiedTimeOfFile(cachePath)

        val sub = nodesList.subList(node.realRDD.indexOfleafInNodesList, node.realRDD.indexOfnodesList+1)
        val cacheNodes = new Array[SimulateRDD](sub.size())
        sub.toArray[SimulateRDD](cacheNodes)
        val indexOfDagScan = new util.ArrayList[Integer]
        println("CacheNodes: ")
        cacheNodes.foreach( t => {
          println(t)
          if ( t.transformation.equalsIgnoreCase("textFile") || t.transformation.equalsIgnoreCase("objectFile")){
            indexOfDagScan.add(cacheNodes.indexOf(t))
          }
        })
        val calendar = Calendar.getInstance()
        val now = calendar.getTime()
        val insertTime = new java.sql.Timestamp(now.getTime())
        val addCache = new CacheMetaData(0, cacheNodes, indexOfDagScan
          , cachePath, modifiedTime, fileSize, (end-begin), insertTime)
        CacheManager.checkCapacityEnoughElseReplace(addCache)
      }
      i -= 1
    }
  }
}
