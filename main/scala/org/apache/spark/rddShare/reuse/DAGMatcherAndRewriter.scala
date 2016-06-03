package org.apache.spark.rddShare.reuse

import java.util
import java.util.function.Consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.rddShare.globalScheduler.JobInformation

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

    println("DAGMatcherAndRewriter.matchTwoDags: ")
    val indexOfDagScan1Ite = indexOfDagScan1.iterator()
    while ( indexOfDagScan1Ite.hasNext ){
      val id1 = indexOfDagScan1Ite.next()
      val indexOfDagScan2Ite = indexOfDagScan2.iterator()
      while ( indexOfDagScan2Ite.hasNext ){
        val id2 = indexOfDagScan2Ite.next()
        var isMatch = true
        var id1Copy = id1
        var id2Copy = id2
        while ( (id1Copy < nodes1.size()) && (id2Copy < nodes2.size()) && isMatch){
          if ( nodes1.get(id1Copy).equals(nodes2.get(id2Copy)) ){
            id1Copy += 1
            id2Copy += 1
          }else{
            isMatch = false
          }
        }
        // match successfully
        if ( id1Copy != id1 ){
          if ( maxMatch < (id1Copy - id1) ){
            println("match success!")
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

  /**
   * this method rewrite the dag
   * @param nodes: the nodes of dag needed to rewrite
   * @param rewrite: the index of rewrite nodes and the path of reusing cache
   */
  def rewriter(nodes: util.ArrayList[SimulateRDD], rewrite: util.ArrayList[(Int, String)]): Unit ={
    rewrite.forEach(new Consumer[(Int, String)] {
      override def accept(re: (Int, String)): Unit = {
        val cachePath = re._2
        val realRDD = nodes.get(re._1).realRDD
        val rewriterRDD = realRDD.sparkContext.objectFile(cachePath, realRDD.partitions.size)
//        val rewriterRDD = realRDD.sparkContext.textFile(cachePath, realRDD.partitions.size)
        val parent = nodes.get(re._1).realRDDparent
        if ( parent == null ){
          realRDD.newRDD = rewriterRDD
        }else{
          parent.changeDependeces(rewriterRDD)
        }
        // update the database
        CacheManager.updatefromDatabase(s"update repository set reuse = reuse + 1 where outputFilename = '$cachePath'")
      }
    })

  }

  /**
   * this method transform a dag to an array based Postorder-Traversal Algorithm
   * @param parent: the parent rdd of the current node
   * @param node: the current node
   * @param nodesList: the nodes of this dag
   * @param indexOfDagScan: the index of nodes which read data
   */
  def transformDAGtoList( parent: RDD[_], node: RDD[_], nodesList: util.ArrayList[SimulateRDD], indexOfDagScan: util.ArrayList[Int] ): Unit = {

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
    nodesList.add(simulateRDD)
    val index = nodesList.indexOf(simulateRDD)
    node.indexOfnodesList = index
    if ( node.transformation.equalsIgnoreCase("textFile") || node.transformation.equalsIgnoreCase("objectFile")  ){
      node.indexOfleafInNodesList = index
      indexOfDagScan.add(index)
      simulateRDD.inputFileName.add(node.name)
      val modifiedTime = CacheManager.getLastModifiedTimeOfFile(node.name)
      simulateRDD.inputFileLastModifiedTime.add(modifiedTime)
    }

    simulateRDD.allTransformation.add(simulateRDD.transformation)
    // pull data from children
    node.dependencies.map(_.rdd).foreach(child => {
      if (node.indexOfleafInNodesList == -1 ){
        node.indexOfleafInNodesList = child.indexOfleafInNodesList
      }
      simulateRDD.inputFileName.addAll(nodesList.get(child.indexOfnodesList).inputFileName)
      simulateRDD.inputFileLastModifiedTime.addAll(nodesList.get(child.indexOfnodesList).inputFileLastModifiedTime)
      simulateRDD.allTransformation.addAll(nodesList.get(child.indexOfnodesList).allTransformation)
    })
  }
}
