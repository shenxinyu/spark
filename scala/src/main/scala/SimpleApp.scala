/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.collection.mutable.{Set, Map}
import java.util._ 
import java.sql.{DriverManager, Connection, Statement}

class DBHelper {  
    val url = "jdbc:mysql://192.168.1.10:3306/friend"
    val name = "com.mysql.jdbc.Driver"
    val user = "root"
    val password = ""
 
    var conn: Connection = null
    var st: Statement = null
  
    Class.forName(this.name)
    conn = DriverManager.getConnection(url, user, password)
    st = conn.createStatement()
  
    def close() = {
            this.conn.close()
            this.st.close()
    }
}

object SimpleApp {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tableName = "friend_bf"
    val db = new DBHelper()
    val ret = db.st.executeQuery("SELECT MAX(id) maxid FROM "+tableName)
    ret.next()
    val maxid = ret.getInt("maxid")
    db.close()
    println(maxid)

    val prop = new Properties();  
    prop.setProperty("user", "root");
    val df = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", tableName, "id", 1, maxid, conf.getInt("spark.cores.max", 1), prop);

    //genarate 1-dim
    val dim1 = df.map(s => (s(1), s(2))).groupByKey().map(s => (s._1, s._2.toSet))
    val dim1_rank = dim1.map(s => (s._1, s._2.size))
    dim1_rank.sortBy(_._2, false).take(10).foreach(println)

    //generate 2-dim
    val dim1_table = sc.broadcast(dim1.collectAsMap)
    val dim2 = dim1.map{
        case(uid, uList) =>
            var a = Set[Any]();
            uList.foreach{fid =>
                 val b = dim1_table.value.get(fid).getOrElse(Set[Any]())
                 a ++= b
            }
            a --= dim1_table.value.get(uid)
            a --= Set(uid)
            val dim2_size = a.size

            var c = Set[Any]();
            a.foreach{fid=>
                 val b = dim1_table.value.get(fid).getOrElse(Set[Any]())
                 c ++= b
            }
            c --= a
            c --= dim1_table.value.get(uid)
            c --= Set(uid)
            val dim3_size = c.size
            (uid, Map("a"->dim2_size, "b"->dim3_size))
    }
    //dim2.take(10).foreach(println)

    val dim2_rank = dim2.map(s => (s._1, s._2("a")))
    dim2_rank.sortBy(_._2, false).take(10).foreach(println)
    val dim3_rank = dim2.map(s => (s._1, s._2("b")))
    dim3_rank.sortBy(_._2, false).take(10).foreach(println)
    }
}

