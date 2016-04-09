/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.delta.DeltaWorkflowManager
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.{Scanner, Calendar, StringTokenizer}

import scala.collection.mutable.MutableList
import scala.io.Source
import scala.reflect.ClassTag

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import org.apache.spark.SparkContext._
import scala.sys.process._





class Test extends userTest[(String, String)] with Serializable {

  def usrTest(inputRDD: RDD[(String, String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass, which returns false
    var returnValue = false

    /*The rest of the code are for correctness test
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/RankedInvertedIndex/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/RankedInvertedIndex.jar", "org.apache.hadoop.examples.RankedInvertedIndex", "-m", "3", "-r", "1",fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[String, List[String]] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/RankedInvertedIndex/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val word  = token.nextToken()
      val docID = token.nextToken()
      if (truthList.contains(word)) {
        var newList = docID::truthList(word)
        newList = newList.reverse
        truthList = truthList updated (word, newList)
      } else {
        truthList = truthList updated (word, List(docID))
      }
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (!truthList.contains(tupVal._1)) returnValue = true
      else {
        val itr2 = tupVal._2.toIterator
        while (itr2.hasNext) {
          val docName = itr2.next()
          if (!truthList(tupVal._1).contains(docName)) {
            returnValue = true
          } else {
            val updateList = truthList(tupVal._1).filter(_ != docName)
            truthList = truthList updated (tupVal._1, updateList)
          }
        }
        if (!truthList(tupVal._1).isEmpty) returnValue = true
        truthList = truthList - tupVal._1
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/RankedInvertedIndex/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    */

    //The following is for test with goNext
    val resultRDD = inputRDD
      .groupByKey()
      .map(pair => {
          //val valueList: MutableList[FileCountPair] = MutableList()
          val valueList: MutableList[String] = MutableList()
          val resultList: MutableList[String] = MutableList()
          //val itr = pair._2.toIterator
          val itr = pair._2.toIterator
          while (itr.hasNext) {
            //val valuePair = new FileCountPair(itr.next())
            val valuePair = new String(itr.next().asInstanceOf[(String, Long)]._1)
            valueList += valuePair
          }
          val valueListArr = valueList.toArray
          val newList = valueListArr.sortWith((a, b) => {
            //if (a.getCount > b.getCount) true
            //else false
            val aColon = a.indexOf("-")
            val bColon = b.indexOf("-")
            val aCount = a.substring(aColon + 1).toInt
            val bCount = b.substring(bColon + 1).toInt
            if (aCount > bCount) true
            else false
          })
          for (a <- newList) {
            val colonIndex = a.indexOf("-")
            val cnt = a.substring(colonIndex + 1)
            //val cnt = a.getCount
            val file = a.substring(0, colonIndex)
            //val rst = cnt + "|" + a.getFile
            val rst = cnt + "|" + file
            resultList += rst
          }
          (pair._1, resultList.toList)
        })
        //this map creates an exception
        .map(pair => {
          var value = new String("")
          for (s <- pair._2) {
            value += s + ","
          }
          value = value.substring(0, value.length - 1)
          if (value.contains("3294446891:4816295")) {
            value += "*"
          }
          (pair._1, value)
        })

    val out = resultRDD.collect()

    //The following is test for version without goNext
//      val resultRDD = inputRDD
//        .map(pair => {
//          val fileIndex = pair._1.lastIndexOf("|")
//          val wordString = pair._1.substring(0, fileIndex)
//          val docId = pair._1.substring(fileIndex + 1)
//
//          //val fileCountPair = new FileCountPair(docId, count)
//          val fileCountPair = new String(docId + "-" + pair._2)
//          (wordString, fileCountPair)
//        })
//        .groupByKey()
//        .map(pair => {
//          //val valueList: MutableList[FileCountPair] = MutableList()
//          val valueList: MutableList[String] = MutableList()
//          val resultList: MutableList[String] = MutableList()
//          //val itr = pair._2.toIterator
//          val itr = pair._2.toIterator
//          while (itr.hasNext) {
//            //val valuePair = new FileCountPair(itr.next())
//            val valuePair = new String(itr.next().asInstanceOf[(String, Long)]._1)
//            valueList += valuePair
//          }
//          val valueListArr = valueList.toArray
//          val newList = valueListArr.sortWith((a, b) => {
//            //if (a.getCount > b.getCount) true
//            //else false
//            val aColon = a.indexOf("-")
//            val bColon = b.indexOf("-")
//            val aCount = a.substring(aColon + 1).toInt
//            val bCount = b.substring(bColon + 1).toInt
//            if (aCount > bCount) true
//            else false
//          })
//          for (a <- newList) {
//            val colonIndex = a.indexOf("-")
//            val cnt = a.substring(colonIndex + 1)
//            //val cnt = a.getCount
//            val file = a.substring(0, colonIndex)
//            //val rst = cnt + "|" + a.getFile
//            val rst = cnt + "|" + file
//            resultList += rst
//          }
//          (pair._1, resultList.toList)
//        })
//        //this map creates an exception
//        .map(pair => {
//          var value = new String("")
//          for (s <- pair._2) {
//            value += s + ","
//          }
//          value = value.substring(0, value.length - 1)
//          if (value.contains("3294446891:4816295")) {
//            value += "*"
//          }
//          (pair._1, value)
//        })
//
//    val out = resultRDD.collect()


//    inputRDD.collect().foreach(println)
//    val finalRdd = DeltaWorkflowManager.generateNewWorkFlow(inputRDD)
//    val out = finalRdd.collect()
    for (o <- out) {
//      println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    return returnValue
  }
}
