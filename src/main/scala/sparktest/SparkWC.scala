package sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark实现单词计数
  * 参数 hdfs://localhost:9000/spark/input/words.text
  * hdfs://localhost:9000/spark/output/
  */
object SparkWC {
    def main(args: Array[String]): Unit = {
        //配置信息类
        val conf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")

        //上下文对象
        val sc = new SparkContext(conf)


        //读取数据
        val lines = sc.textFile(args(0))

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val paired: RDD[(String, Int)] = words.map((_,1))
        val reduced: RDD[(String, Int)] = paired.reduceByKey(_+_)
        val res: RDD[(String, Int)] = reduced.sortBy(_._2,false)

//        res.saveAsTextFile(args(1))

        println(res.collect().toBuffer)

    }

}
