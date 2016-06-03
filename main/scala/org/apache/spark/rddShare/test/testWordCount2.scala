package org.apache.spark.rddShare.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcq on 16-5-10.
 */
object testWordCount2 {

  private val conf = new SparkConf().setAppName("WordCount2")
//    .setMaster("local")
    .setMaster("spark://192.168.1.105:7077")
    .set("spark.shuffle.service.enabled", "true")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/hcq/Documents/spark_1.6.1/eventLog")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  private val sc = new SparkContext(conf)

  def main(args: Array[String]) {
//    CacheManager.initRepository
  // big input size
//    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/Rankings/").flatMap(line => line.split(" ")).
//      map(word => (word, 1)).reduceByKey(_ + _)
    // small input size
    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/part-00001").flatMap(line => line.split(" ")).
        map(word => (word, 1)).reduceByKey(_ + _)
    // test replace cache
//    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/Rankings/part-00003").flatMap(line => line.split(" ")).
//        map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.saveAsTextFile("/home/hcq/Documents/spark_1.5.0/output/"+ System.currentTimeMillis()+wordCount.transformation)

    stopSpark()
  }

  def stopSpark() {
//    CacheManager.saveRepository
    sc.stop()
  }

}
