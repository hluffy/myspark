package sparktest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义二次排序
  */

//第一种排序方式
object MySort{
    implicit val girlOrdering = new Ordering[Girl]{
        override def compare(x: Girl, y: Girl): Int = {
            if (x.faceValue != y.faceValue){
                x.faceValue - y.faceValue
            }else{
                if(x.age>y.age){
                    x.age - y.age
                }else{
                    y.age - x.age
                }
            }
        }
    }
}

object CustomSort {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("CustomSOrt").setMaster("local")
        val sc = new SparkContext(conf)

        val girlinfo = sc.parallelize(Array(("tingting",88,25),("ninging",90,26),("mimi",90,27)))

//        val res = girlinfo.sortBy(_._2,false)
//        println(res.collect.toBuffer)

        //第一中排序方式
//        import MySort.girlOrdering
//        val res = girlinfo.sortBy(x => Girl(x._2,x._3),false)
//        println(res.collect.toBuffer)

        //第二种排序方式
        val res = girlinfo.sortBy(x => Girl(x._2,x._3),false)
        println(res.collect.toBuffer)

        sc.stop
    }

}

//第一种排序方式
//case class Girl(faceValue:Int,age:Int)

//第二种排序方式
case class Girl(faceValue:Int,age:Int) extends Ordered[Girl]{
    override def compare(that: Girl): Int = {
        if(this.faceValue != that.faceValue){
            this.faceValue - that.faceValue
        }else{
            this.age - this.age
        }
    }
}
