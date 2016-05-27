package org.apache.spark.rddShare.globalScheduler

import java.util.Random

import scala.util.Sorting

/**
 * Created by hcq on 16-5-26.
 */
class GA(val scale: Int,      // the scale of this population
         val jobNum: Int,    // the number of dags
         val maxLen: Int,    // the max num of iterating
         val ps: Double,      // the probability of select
         val pc: Double,      // the probability of cross
         val pm: Float,       // the probability of mutate
         val wr: Float,       // the weight of reuse in fitness function
         val wl: Float        // the weight of location in fitness function
          ) {

  private var nowLen = 0     // current iterate num
  private val reuse = Array.ofDim[Double](jobNum, jobNum)   // the matrix of reusing between all the dags
  private val bestResult = new Array[Int](jobNum)           // the best individual in population
  private val bestSeq = Array.ofDim[Int](maxLen, jobNum)    // the seq of best individual in population
  private val bestFit = new Array[Double](maxLen)           // the fitness corresponding of the Best_seq

  private val parentPopulation = Array.ofDim[Int](scale, jobNum)
  private val childPopulation = Array.ofDim[Int](scale, jobNum)

  private val fitness = new Array[Double](scale)
  private val random = new Random(System.currentTimeMillis)

  // compute the matrix reusing base the jobs information
  def computeReuseMatrix(jobs: Array[JobInformation]): Unit ={

  }

  def initPopulation: Unit ={
    for ( i <- 0 to scale ){
      var end = jobNum - 1
      val indexArray = Array.range(0, jobNum)
      for ( j <- 0 to jobNum){
        val index = random.nextInt(end + 1)
        parentPopulation(i)(j) = indexArray(index)
        indexArray(index) = indexArray(end)
        end -= 1
      }
    }
  }

  // calculate the sum reuse and the location between jobs in an individual
  private def sumReuseAndLocationOfIndividual(individual: Array[Int]): (Double, Int) ={
    var sumReuse = .0
    var sumLocation = 0
    for ( i <- 0 to jobNum-1){
      var maxReuse = .0
      var maxLocation = 0
      for ( j <- i+1 to jobNum ){
        // fetch the data that can be reuse by individual(i)
        if ( maxReuse < reuse(individual(i))(individual(j)) ){
          maxReuse = reuse(individual(i))(individual(j))
          maxLocation = j - i
        }
      }
      // sum the max data that can be reuse by individual(i)
      sumReuse += maxReuse
      sumLocation += maxLocation
    }
    (sumReuse, sumLocation)
  }

  // calculating the fitness of every individual in population
  private def calculateFitness: Unit ={
    for ( i <- 0 to scale){
      val (reuse, location) = sumReuseAndLocationOfIndividual(parentPopulation(i))
      fitness(i) = reuse * wr + location * wl
    }
  }

  private def select: Unit ={
    val copyFitness = fitness.clone()
    Sorting.quickSort(fitness)  // sort ascending with the fitness
    var i = scale-1
    while ( i >= scale*(1-ps) ){
      for ( j <- 0 to scale ){
        if ( copyFitness(j).equals(fitness(i))){
          childPopulation(scale-1-i) = parentPopulation(j)
          if ( i == scale-1 ){
            bestSeq(nowLen) = parentPopulation(j)
            bestFit(nowLen) = fitness(i)
            nowLen += 1
          }
        }
      }
      i -= 1
    }
    i = (scale*ps).toInt
    while ( i >= 0){
      childPopulation(i) = childPopulation(random.nextInt( (scale*ps).toInt)
        + (scale*(1-ps)).toInt + 1)
      i -= 1
    }
  }
}
