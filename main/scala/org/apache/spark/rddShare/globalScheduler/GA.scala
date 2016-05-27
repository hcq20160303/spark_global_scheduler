package org.apache.spark.rddShare.globalScheduler

import java.util.Random

import org.apache.spark.rddShare.reuse.DAGMatcherAndRewriter

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
    for ( i <- 0 to (jobNum - 1) ){
      for ( j <- i+1 to jobNum){
        val (jobiMatchIndex, _) = DAGMatcherAndRewriter.matchTwoDags(jobs(i), jobs(j))
        // the calculating way of reuse is the number of match nodes divided the number of nodes in a job
        reuse(i)(j) = (jobiMatchIndex._2 - jobiMatchIndex._1)/(jobs(i).nodes.length)
        reuse(j)(i) = reuse(i)(j)
      }
    }
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

  private def rotate( newPopulation: Array[Array[Int]],row: Int, column: Int, num: Int): Unit = {
    val temp = newPopulation.clone()
    Array.copy(temp(row), column, newPopulation(row), num, jobNum-column)
    Array.copy(temp(row), num, newPopulation(row), jobNum-column, column-num)
  }

  private def cross: Unit ={
    var ran1, ran2, temp, flag1, flag2 = 0
    var max1, max2 = .0

    val tempPopulation = childPopulation.clone()
    val fitReuse = Array.ofDim[Double](2, jobNum)
    val fitLocation = Array.ofDim[Int](2, jobNum)
    val fit = Array.ofDim[Double](2, jobNum)

    for(i <- 0 to (scale*pc).toInt ){
      ran1=random.nextInt(jobNum)
      ran2=random.nextInt(jobNum)
      while(tempPopulation(ran1).containsSlice(tempPopulation(ran2)) ){
        ran2=random.nextInt(jobNum)
      }
      if (fitness(ran2) > fitness(ran1)) {
        temp = ran1
        ran1 = ran2
        ran2 = temp
      }

      for( j <- 0 to jobNum ){
        for( k <- 0 to jobNum if j !=k ){
          fitReuse(0)(j) = fitReuse(0)(j) + reuse(tempPopulation(ran1)(j))(tempPopulation(ran1)(k))
          fitReuse(1)(j) = fitReuse(1)(j) + reuse(tempPopulation(ran2)(j))(tempPopulation(ran2)(k))
          if( reuse(tempPopulation(ran1)(j))(tempPopulation(ran1)(k)) != 0 )
            fitLocation(0)(j) = fitLocation(0)(j) + math.abs(j-k)
          if( reuse(tempPopulation(ran2)(j))(tempPopulation(ran2)(k)) != 0 )
            fitLocation(1)(j) = fitLocation(1)(j) + math.abs(j-k)
        }
        fit(0)(j) = fitReuse(0)(j)*wr + fitLocation(0)(j)*wl
        fit(1)(j) = fitReuse(0)(j)*wr + fitLocation(0)(j)*wl
        if( fit(0)(j) > max1 ){
          max1 = fit(0)(j)
          flag1=j
        }
        if( fit(1)(j) > max2 ){
          max2 = fit(1)(j)
          flag2 = j
        }
      }

      if( max1 >= max2 ){
        childPopulation(ran2)(i) = tempPopulation(ran1)(i)
        rotate(tempPopulation, ran2, flag2, i)
      }
      else{
        childPopulation(ran2)(i)=tempPopulation(ran2)(i)
        rotate(tempPopulation, ran1, flag1, i)
      }
    }
  }

  private def mutate: Unit = {
    val label = new Array[Int]((scale * pm).toInt)
    var ran1, ran2, temp = 0
    for (i <- 0 to (scale * pm).toInt) {
      label(i) = random.nextInt(scale)
      var j = 0
      while (j < i) {
        if (label(i) == label(j)) {
          label(i) = random.nextInt(jobNum)
          j = 0
        }
      }
      ran1 = random.nextInt(jobNum)
      ran2 = random.nextInt(jobNum)
      while (ran1 == ran2) {
        ran2 = random.nextInt(jobNum)
      }
      temp = childPopulation(label(i))(ran1)
      childPopulation(label(i))(ran1) = childPopulation(label(i))(ran2)
      childPopulation(label(i))(ran2) = temp
    }
  }

  def iteration: Array[Int] = {
    val temp = new Array[Double](maxLen)
    initPopulation
    for (i <- 0 to maxLen) {
      calculateFitness
      select
      cross
      mutate
      childPopulation.copyToArray(parentPopulation)
    }
    bestFit.copyToArray(temp)
    Sorting.quickSort(bestFit)
    for (i <- 0 to maxLen) {
      if (temp(i) == bestFit(maxLen - 1)) {
        bestSeq(i).copyToArray(bestResult)
      }
    }
    bestResult
  }
}
