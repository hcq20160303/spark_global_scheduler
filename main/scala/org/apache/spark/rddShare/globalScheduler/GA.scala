package org.apache.spark.rddShare.globalScheduler

import java.util.Random

import org.apache.spark.rddShare.reuse.DAGMatcherAndRewriter

import scala.util.Sorting

/**
 * Created by hcq on 16-5-26.
 */
class GA( val jobNum: Int    // the number of dags
          ) {

  private val scale: Int = 100       // the scale of this population
  private val maxLen: Int = 10       // the max num of iterating
  private val ps: Double = 0.03      // the probability of select
  private val pc: Double = 0.03      // the probability of cross
  private val pm: Double = 0.02     // the probability of mutate
  private val wr: Double = 0.6       // the weight of reuse in fitness function
  private val wl: Double = 0.4       // the weight of location in fitness function

  private var nowLen = 0     // current iterate num
  val reuse = Array.ofDim[Double](jobNum, jobNum)   // the matrix of reusing between all the dags
  private val bestResult = new Array[Int](jobNum)           // the best individual in population
  private val bestSeq = Array.ofDim[Int](maxLen, jobNum)    // the seq of best individual in population
  private val bestFit = new Array[Double](maxLen)           // the fitness corresponding of the Best_seq

  private val parentPopulation = Array.ofDim[Int](scale, jobNum)
  private val childPopulation = Array.ofDim[Int](scale, jobNum)

  private val fitness = new Array[Double](scale)
  private val random = new Random(System.currentTimeMillis)

  // compute the matrix reusing base the jobs information
  def computeReuseMatrix(jobs: Array[JobInformation]): Unit ={
    println("GA.computeReuseMatrix")
    for ( i <- 0 to (jobNum - 2) ){
      for ( j <- i+1 to jobNum - 1){
        val (jobiMatchIndex, _) = DAGMatcherAndRewriter.matchTwoDags(jobs(i), jobs(j))
        // the calculating way of reuse is the number of match nodes divided the number of nodes in a job,
        // as we can't get the information of the input data, so we simply compute the reusing in this way
        reuse(i)(j) = (jobiMatchIndex._2 - jobiMatchIndex._1 + 1)/(jobs(i).nodes.length)
        reuse(j)(i) = reuse(i)(j)
        println("reuse(i)(j): "+reuse(i)(j))
      }
    }
  }

  /**
   * this method just for test the GA algorithm
   */
  def initReuseMatrix: Unit ={
    for ( i <- 0 to (jobNum - 2) ){
      for ( j <- i+1 to jobNum - 1){
        reuse(i)(j) = random.nextDouble()
        reuse(j)(i) = reuse(i)(j)
      }
    }
  }

  private def initPopulation: Unit ={
    for ( i <- 0 to scale-1 ){
      var end = jobNum - 1
      val indexArray = Array.range(0, jobNum)
      for ( j <- 0 to jobNum-1){
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
    for ( i <- 0 to jobNum-2){
      var maxReuse = .0
      var maxLocation = 0
      for ( j <- i+1 to jobNum-1 ){
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
    for ( i <- 0 to scale-1){
      val (reuse, location) = sumReuseAndLocationOfIndividual(parentPopulation(i))
      fitness(i) = reuse * wr + location * wl
    }
  }

  private def select: Unit ={
    val copyFitness = fitness.clone()
    Sorting.quickSort(fitness)  // sort ascending with the fitness
    var i = scale-1
    while ( i >= scale*(1-ps) ){
      for ( j <- 0 to scale-1 ){
        if ( copyFitness(j).equals(fitness(i))){
          childPopulation(scale-1-i) = parentPopulation(j)
          /**
           * nowLen have some problem, it will become big than maxLen
           */
          if ( i == scale-1 && nowLen < maxLen ){
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
//      childPopulation(i) = childPopulation(random.nextInt( (scale*ps).toInt)
//        + (scale*(1-ps)).toInt )
      childPopulation(i) = parentPopulation(random.nextInt( (scale*ps).toInt)
        + (scale*(1-ps)).toInt )
      i -= 1
    }
    println("GA.select: I have back from while...")
  }

  private def rotate( newPopulation: Array[Array[Int]],row: Int, column: Int, num: Int): Unit = {
    val temp = newPopulation.clone()
    println("row: "+row+"\tcolumn: "+column+"\tnum: "+num+"\tjobNum-column: "+(jobNum-column))
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
      println("GA.cross: I have back from while...")
      if (fitness(ran2) > fitness(ran1)) {
        temp = ran1
        ran1 = ran2
        ran2 = temp
      }

      for( j <- 0 to jobNum-1 ){
        for( k <- 0 to jobNum-1 if j !=k ){
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
        if ( i < jobNum){
          childPopulation(ran2)(i) = tempPopulation(ran1)(i)
          rotate(tempPopulation, ran2, flag2, i)
        }
      }
      else{
        if ( i < jobNum) {
          childPopulation(ran2)(i) = tempPopulation(ran2)(i)
          rotate(tempPopulation, ran1, flag1, i)
        }
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
        j += 1
      }
      println("GA.mutate: I have back from while 1...")
      ran1 = random.nextInt(jobNum)
      ran2 = random.nextInt(jobNum)
      while (ran1 == ran2) {
        ran2 = random.nextInt(jobNum)
      }
      println("GA.mutate: I have back from while 2...")
      temp = childPopulation(label(i))(ran1)
      childPopulation(label(i))(ran1) = childPopulation(label(i))(ran2)
      childPopulation(label(i))(ran2) = temp
    }
  }

  def iteration: Array[Int] = {
    val temp = new Array[Double](maxLen)
    initPopulation
    for (i <- 0 to maxLen-1) {
      calculateFitness
      select
      cross
      mutate
      childPopulation.copyToArray(parentPopulation)
    }
    bestFit.copyToArray(temp)
    Sorting.quickSort(bestFit)
    for (i <- 0 to maxLen-1) {
      if (temp(i) == bestFit(maxLen - 1)) {
        bestSeq(i).copyToArray(bestResult)
      }
    }
    println("GA.iteration: finished iteration.")
    bestResult
  }
}
