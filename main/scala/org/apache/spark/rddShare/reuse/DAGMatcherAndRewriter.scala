package org.apache.spark.rddShare.reuse

import org.apache.spark.rdd.RDD
import org.apache.spark.rddShare.globalScheduler.JobInformation

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hcq on 16-5-9.
 *
 * 匹配及改写：该object将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
 */

object DAGMatcherAndRewriter {

  // match two dags and get the index of match position in two dags
  def matchTwoDags(dag1: JobInformation, dag2: JobInformation): ( (Int, Int), (Int, Int)) ={
    val indexOfDagScan1 = dag1.indexOfDagScan
    val nodes1 = dag1.nodes
    val indexOfDagScan2 = dag2.indexOfDagScan
    val nodes2 = dag2.nodes
    // get the match index of dag1 with dag2
    var dag1MatchBegin, dag1MatchEnd = 0
    var dag2MatchBegin, dag2MatchEnd = 0
    var maxMatch = -1

    for ( id1 <- indexOfDagScan1 ){
      for ( id2 <- indexOfDagScan2){
        var isMatch = true
        var id1Copy = id1
        var id2Copy = id2
        while ( (id1Copy < nodes1.length) && (id2Copy < nodes2.length) && isMatch){
          if ( nodes1(id1Copy).equals(nodes2(id2Copy)) ){
            id1Copy += 1
            id2Copy += 1
          }else{
            isMatch = false
          }
        }
        // match successfully
        if ( id1Copy != id1 ){
          if ( maxMatch < (id1Copy - id1) ){
            maxMatch = id1Copy - id1
            dag1MatchBegin = id1
            dag1MatchEnd = id1Copy - 1
            dag2MatchBegin = id2
            dag2MatchEnd = id2Copy - 1
          }
        }
      }
    }
    ( (dag1MatchBegin, dag1MatchEnd), (dag2MatchBegin, dag2MatchEnd))
  }

//  def dagMatcherAndRewriter(rddShare: RDDShare, finalRDD: RDD[_], nodesList: ArrayList[SimulateRDD], indexOfDagScan: ArrayList[Integer]): Unit = {
//    transformDAGtoList(null, finalRDD, nodesList, indexOfDagScan)
//    val repository = CacheManager.getRepository
//    println("DAGMatcherAndRewriter.dagMatcherAndRewriter: CacheManager.getRepository")
//    repository.forEach(new Consumer[CacheMetaData] {
//      override def accept(t: CacheMetaData): Unit = {
//        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
//          "sizoOfOutputData: " + t.sizeOfOutputData + "\tuse: " + t.reuse)
//      }
//    })
//    if ( repository.size() != 0 ){
//      /**
//       * 将输入dag和仓库一一进行匹配
//       */
//      val ite = repository.iterator()
//      while ( ite.hasNext ){
//        val cacheMetaData = ite.next()
//        val indexOfCacheDagScan = cacheMetaData.indexOfDagScan
//        val cacheNodesList = cacheMetaData.nodesList
//        if ( nodesList.size() >= cacheNodesList.length && indexOfDagScan.size() >= indexOfCacheDagScan.size()) {
//          /**
//           * 将cache和DAG中的每个Load操作符进行比较
//           */
//          val ite = indexOfDagScan.iterator()
//          val hasCheck = new ArrayList[Integer]()
//          var isMatch = true
//          while ( ite.hasNext ) {
//            val idOfDagScan = ite.next()
//            hasCheck.add(idOfDagScan)
////            indexOfDagScan.remove(idOfDagScan)
//            /**
//             * Matcher
//             */
//            var index: Int = 0  // cache中Scan操作的位置
//            /**
//             * bug9: 对于dag中有多个输入的情况，当有一个输入匹配成功，那么后一个输入在dag中的位置则会改变
//             * 因此，变量indexOfDagScan不是固定的，需要根据匹配情况更改.
//             */
//            var indexOfdag: Int = idOfDagScan  // dag中Scan操作的位置（可能有多个Scan操作）
//            println("DAGMatcherAndRewriter.dagMatcherAndRewriter: ")
//            while (index < cacheNodesList.length && isMatch) {
//              if (cacheNodesList(index).equals(nodesList.get(indexOfdag))) {
//                index = index+1
//                indexOfdag = indexOfdag+1
//              } else {
//                println("cacheNodesList(index)): " + cacheNodesList(index))
//                println("nodesList.get(indexOfdag): " + nodesList.get(indexOfdag))
//                isMatch = false
//              }
//            }
//            /**
//             * Rewriter
//             */
//            println("DAGMatcherAndRewriter.scala---isMatch: " + isMatch)
//            if (isMatch) {   // 完全匹配则改写DAG
//              // check if the input files and the output file is exist
//              // 1. check input files
//              var inputFileExist = true
//              cacheMetaData.root.inputFileName.forEach(new Consumer[String] {
//                override def accept(t: String): Unit = {
//                  if ( !CacheManager.fileExist(t, "input")){
//                    inputFileExist = false
//                  }
//                }
//              })
//              // 2. check output files
//              if ( inputFileExist && CacheManager.fileExist(cacheMetaData.outputFilename, "output") ){
//                // after check exist, then check if these files have modified by others
//                println("DAGMatcherAndRewriter.scala---file exist: " + true)
//                if ( CacheManager.checkFilesNotModified(cacheMetaData)){
//                  println("DAGMatcherAndRewriter.scala---file modified: " + false)
//                  println("DAGMatcherAndRewriter.scala---outputFilename: " + cacheMetaData.outputFilename)
//                  val realRDD = nodesList.get(indexOfdag - 1).realRDD
//                  val rewriter = finalRDD.sparkContext.objectFile(cacheMetaData.outputFilename, realRDD.partitions.size)
//                  val parent = nodesList.get(indexOfdag - 1).realRDDparent
//                  if ( parent == null){
//                    rddShare.setFinalRDD(rewriter)
//                  } else {
//                    parent.changeDependeces(rewriter)
//                  }
//                  cacheMetaData.reuse += 1   // use add 1 to judge if it should be replaced for future
//                  val newReuse = cacheMetaData.reuse
//                  val id = cacheMetaData.id
//                  CacheManager.updatefromDatabase(s"update repository set reuse = reuse + 1 where id = $id")
//                }else{
//                  println("DAGMatcherAndRewriter.scala---file modified: " + true)
//                }
//              }else{
//                println("DAGMatcherAndRewriter.scala---file exist: " + false)
//              }
//            }
//          }
//          if ( isMatch ){
//            indexOfDagScan.removeAll(hasCheck)
//          }
//          hasCheck.clear()
//        }
//      }
//    }
//    // the dependencies of finalRdd may be changed, so we need transform it again
//    nodesList.clear()
//    indexOfDagScan.clear()
//    transformDAGtoList(null, rddShare.getFinalRDD, nodesList, indexOfDagScan)
//  }

  /**
   * this method rewrite the dag
   * @param nodes: the nodes of dag needed to rewrite
   * @param rewrite: the index of rewrite nodes and the path of reusing cache
   */
  def rewriter(nodes: Array[SimulateRDD], rewrite: Array[(Int, String)]): Unit ={
    rewrite.foreach( re => {
      val cachePath = re._2
      val realRDD = nodes(re._1).realRDD
      val rewriterRDD = realRDD.sparkContext.objectFile(cachePath, realRDD.partitions.size)
      val parent = nodes(re._1).realRDDparent
      if ( parent == null ){
        realRDD.newRDD = rewriterRDD
      }else{
        parent.changeDependeces(rewriterRDD)
      }
      // update the database
      CacheManager.updatefromDatabase(s"update repository set reuse = reuse + 1 where outputFilename = $cachePath")
    })
  }

  /**
   * this method transform a dag to an array based Postorder-Traversal Algorithm
   * @param parent: the parent rdd of the current node
   * @param node: the current node
   * @param nodesList: the nodes of this dag
   * @param indexOfDagScan: the index of nodes which read data
   */
  def transformDAGtoList( parent: RDD[_], node: RDD[_], nodesList: ArrayBuffer[SimulateRDD], indexOfDagScan: ArrayBuffer[Int] ): Unit = {

    if ( node == null ){
      return
    }

    if ( node.dependencies != null ) {
      println("rdd" + node.id + "'s dependencies: " + node.dependencies.toString())
      node.dependencies.map(_.rdd).foreach(child => transformDAGtoList(node, child, nodesList, indexOfDagScan))
    }

    val simulateRDD = new SimulateRDD(node.transformation, node.function)
    simulateRDD.realRDD = node

    // judge if the rdd's transformation is read data
    nodesList += simulateRDD
    val index = nodesList.indexOf(simulateRDD)
    node.indexOfnodesList = index
    if ( node.transformation.equalsIgnoreCase("textFile") || node.transformation.equalsIgnoreCase("objectFile")  ){
      node.indexOfleafInNodesList = index
      indexOfDagScan += index
      simulateRDD.inputFileName.add(node.name)
      val modifiedTime = CacheManager.getLastModifiedTimeOfFile(node.name)
      simulateRDD.inputFileLastModifiedTime.add(modifiedTime)
    }

    simulateRDD.allTransformation.add(simulateRDD.transformation)
    // pull data from childs
    node.dependencies.map(_.rdd).foreach(child => {
      if (node.indexOfleafInNodesList == -1 ){
        node.indexOfleafInNodesList = child.indexOfleafInNodesList
      }
      simulateRDD.inputFileName.addAll(nodesList(child.indexOfnodesList).inputFileName)
      simulateRDD.inputFileLastModifiedTime.addAll(nodesList(child.indexOfnodesList).inputFileLastModifiedTime)
      simulateRDD.allTransformation.addAll(nodesList(child.indexOfnodesList).allTransformation)
    })
  }
}
