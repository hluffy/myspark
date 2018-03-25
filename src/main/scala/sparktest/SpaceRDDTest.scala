package sparktest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark常用算子练习
  */
object SpaceRDDTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkRDDTest").setMaster("local")
        val sc = new SparkContext(conf)

        //通过并行生成rdd
        val rdd1 = sc.parallelize(List(5,6,4,7,3,8,2,9,1,10))
        //对rdd1里的每个元素乘以2然后排序
        val res1 = rdd1.map(_ * 2).sortBy(x => x,true)
//        println(res1.collect().toBuffer)

        //过滤处大于等于十的元素
        val res2 = res1.filter(_ >= 10)

        //将元素以数组的方式打印出来
        println(res2.collect.toBuffer)

        val rdd2 = sc.parallelize(Array("a b c","d e f","h i j"))
        //将rdd2里的元素先切分在压平
        val res3 = rdd2.flatMap(_.split(" "))
//        println(res3.collect.toBuffer)

        val rdd3 = sc.parallelize(List(List("a b c","a b b"),List("e f g","a f g"),List("h i j","a b b")))
        val res4 = rdd3.flatMap(_.flatMap(_.split(" ")))
//        println(res4.collect.toBuffer)

        //求并集
        val rdd4 = sc.parallelize(List(5, 6, 4, 3))
        val rdd5 = sc.parallelize(List(1, 2, 3, 4))
        val unionRes = rdd4 union rdd5
//        println(unionRes.collect.toBuffer)

        //求交集
        println((rdd4 intersection rdd5).collect.toBuffer)

        //去重
        println(unionRes.distinct.collect.toBuffer)

        val rdd6 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
        val rdd7 = sc.parallelize(List(("jerry",2),("tom",1),("shuke",2)))
        //求join
        println(rdd6.join(rdd7).collect.toBuffer)

        //求左连接和右连接
        println(rdd6.leftOuterJoin(rdd7).collect.toBuffer)
        println(rdd6.rightOuterJoin(rdd7).collect.toBuffer)

        //求并集
        val res = rdd6 union rdd7
//        println(rescollect.toBuffer)

        //按key进行分组
        println(res.groupByKey().collect.toBuffer)


        //分别使用groupByKey和reduceByKey进行单词计数
        println(res.groupByKey.mapValues(_.sum).collect.toBuffer)
        println(res.reduceByKey(_+_).collect.toBuffer)

        val rdd8 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
        val rdd9 = sc.parallelize(List(("jerry",2),("tom",1),("shuke",2)))
        //cogroup,注意和groupByKey的区别
        println(rdd8.cogroup(rdd9).collect.toBuffer)

        val rdd10 = sc.parallelize(List(1, 2, 3, 4, 5))
        //reduce聚合
        println(rdd10.reduce(_+_))

        val rdd11 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
        val rdd12 = sc.parallelize(List(("jerry",2),("tom",1),("shuke",2)))
        val rdd13 = rdd11 union rdd12

        //按key进行聚合
        //reduceByKey

        //按value的降序排序
        val res12 = rdd13.reduceByKey(_+_).sortBy(_._2,false)
        println(res12.collect.toBuffer)
        val res13 = rdd13.reduceByKey(_+_).map(t => (t._2,t._1)).sortByKey(false).map(t => (t._2,t._1))
        println(res13.collect.toBuffer)

        //笛卡尔积
        println((rdd11 cartesian rdd12).collect.toBuffer)



    }

}
