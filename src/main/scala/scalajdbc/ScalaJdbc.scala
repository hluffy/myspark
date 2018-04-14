package scalajdbc

import java.sql.{Connection, DriverManager, ResultSet}

object ScalaJdbc {
    def main(args: Array[String]): Unit = {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost/helemet"
        val username = "root"
        val password = "root"

        var conn:Connection = null

        try {
            Class.forName(driver)
            conn = DriverManager.getConnection(url,username,password)

            val sql = "select count(*) as countsum from map_watch_data_act"
            val statement = conn.createStatement()
            val rs:ResultSet = statement.executeQuery(sql)
            while(rs.next()){
                println(rs.getInt("countsum"))
            }
        } catch {
            case e => e.printStackTrace()
        } finally {
            if(conn!=null){
                conn.close()
            }

        }


    }

}
