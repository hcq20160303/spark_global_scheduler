package org.apache.spark.rddShare.globalScheduler

import java.util.Random

/**
 * Created by hcq on 16-5-26.
 */
class GA(val scale: Int,      // the scale of this population
         val DAG_num: Int,    // the number of dags
         val Max_len: Int,    // the max num of iterating
         val Ps: Double,      // the probability of select
         val Pc: Double,      // the probability of cross
         val Pm: Float,       // the probability of mutate
         val Wr: Float,       // the weight of reuse in fitness function
         val Wl: Float        // the weight of location in fitness function
          ) {

  private var Now_len = 0     // current iterate num
  private val reuse = new Array[Array[Int]](DAG_num)      // the matrix of reusing between all the dags
  private val Best_result = new Array[Int](DAG_num)       // the best individual in population
  private val Best_seq = new Array[Array[Int]](Max_len)   // the seq of best individual in population
  private val Best_fit = new Array[Double](Max_len)       // the fitness corresponding of the Best_seq

  private val Parent_population = new Array[Array[Int]](scale)
  private val Child_population = new Array[Array[Int]](scale)

  private val fitness = new Array[Float](scale)
  private val random = new Random(System.currentTimeMillis)

  // compute the matrix reusing base the jobs information
  def computeReuseMatrix(jobs: Array[JobInformation]): Unit ={

  }

}
