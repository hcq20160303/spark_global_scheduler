package org.apache.spark.rddShare.tool

import java.io._

import com.typesafe.config.ConfigFactory

/**
 * Created by hcq on 16-5-7.
 */
object MyUtils {

  val sparkCorePath = getClass.getResource("").getPath.split("target")(0)
  val resourcesPath = sparkCorePath + "src/main/resources/rddShare/"
  val conf = ConfigFactory.parseFile(new File(resourcesPath + "default.conf"))

  def getFunctionOfRDD(input: InputStream, funClassPath: String): String ={
//    var function = ""
//    val pathOfFunctionJava = RDDShare.getAnnoFunctionCopyPath+funClassPath+".class"
//    // copy the .class file to pathOfFunctionClass
//    MyFileIO.fileWriteBytes(pathOfFunctionJava, MyFileIO.fileReadBytes(input))
//    // recomplie the .class file to .java file use jad
//    val command = "jad -sjava " + pathOfFunctionJava
//    Process(command).!
//
//    val pathOfJavaFile = pathOfFunctionJava.split(".cla")(0) + ".java"
//    val sources = Source.fromFile(pathOfJavaFile).getLines().toIterator
//    var findApply = false
//    while ( sources.hasNext ){
//      val line = sources.next()
//      if ( line.contains("apply") ){
//        findApply = true
//      }
//      if ( findApply ) {
//        function += line
//      }
//    }
//    function
    "function"
  }

  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  def printArrayList(list: Array[AnyRef]): Unit ={
    list.foreach(println)
  }
}
