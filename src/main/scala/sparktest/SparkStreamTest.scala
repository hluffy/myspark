package sparktest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkStreamTest").setMaster("local[*]")
        val ssc = new StreamingContext(conf,Seconds(1))

        val lines = ssc.socketTextStream("localhost",9999)
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map((_,1))
        val wordCounts = pairs.reduceByKey(_+_)
        wordCounts.print

        ssc.start()
        ssc.awaitTermination()
    }

}
