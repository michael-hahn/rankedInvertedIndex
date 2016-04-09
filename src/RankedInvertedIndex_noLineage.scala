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

/**
 * Created by Michael on 4/5/16.
 */
object RankedInvertedIndex_noLineage {
  private val exhaustive = 0



  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[6]")
      sparkConf.setAppName("RankedInvertedIndex_LineageDD")
        .set("spark.executor.memory", "2g")


      //set up lineage
//      var lineage = true
//      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
//      if (args.size < 2) {
//        logFile = "test_log"
//        lineage = true
//      } else {
//        lineage = args(0).toBoolean
//        logFile += args(1)
//        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
//      }


      val ctx = new SparkContext(sparkConf)

      //set up lineage context and start capture lineage in the following spark program
//      val lc = new LineageContext(ctx)


      /* For correctness test only (produce the ground truth file)
      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/RankedInvertedIndex.jar", "org.apache.hadoop.examples.RankedInvertedIndex", "-m", "3", "-r", "1","/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii_file", "output").!!
      */

      //start recording the time for lineage
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val lines = ctx.textFile("/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii", 1)

      //At this stage, technically lineage has already find all the faulty data set, we record the time
//      val lineageEndTime = System.nanoTime()
//      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " microseconds")
//      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

      //      linRdd.show.collect.foreach(println)

      //      linRdd.show.collect().foreach(s => {
      //        pw.append(s.toString)
      //        pw.append('\n')
      //      })

      //      pw.close()


//      val showMeRdd = linRdd.show().toRDD
      val mappedRDD = lines.map(s => {
        val line = s.toString()
        val countIndex = line.lastIndexOf("\t")
        val valueString = line.substring(0, countIndex)
        val count = line.substring(countIndex + 1)
        (valueString, count)
      })
      mappedRDD.cache()


      //      println("MappedRDD has " + mappedRDD.count() + " records")


      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/InvertedIndex/lineageResult", 1)
      //      val lineageResult = ctx.textFile("/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii_file", 1)
      //
      //      val num = lineageResult.count()
      //      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/RankedInvertedIndex/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
      //lineageResult.cache()

      //this version is for test without goNext
      val delta_debug = new DD_NonEx_v2[(String, String)]
      val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split_v2, lm, fh)


      val ss = returnedRDD.collect.foreach(println)

//      ss.foreach(println)

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")

      //total time
      logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (DeltaDebuggingEndTime - LineageStartTime)/1000 + " microseconds")


      println("Job's DONE!Works!")
      ctx.stop()

    }
  }

}
