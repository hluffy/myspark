package scalahbase

import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * scala连接hbase
  * 待解决
  * class not found exception : org.apache.hadoop.conf.Configuration
  */
object HBaseOp {
    def main(args: Array[String]): Unit = {

       val config =  HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum","localhost")
        println(config)
    }
}



class HBaseOp {

}
