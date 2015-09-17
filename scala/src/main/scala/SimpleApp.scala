/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import java.util._ 
import scala.collection.mutable.Set
import scala.collection.mutable.Map

object SimpleApp {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val prop = new Properties();  
    prop.setProperty("user", "root");
    val df = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", "friend", "id", 1, 20000000, conf.get("spark.cores.max").toInt, prop);
    //val df = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", "friend", Array("id<80"), prop);
    //df.registerTempTable("friend")
    //val results = df.sqlContext.sql("SELECT uid, fid FROM friend")

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
    //dim2.cache()
    //dim2.take(10).foreach(println)

    val dim2_rank = dim2.map(s => (s._1, s._2("a")))
    dim2_rank.sortBy(_._2, false).take(10).foreach(println)
    val dim3_rank = dim2.map(s => (s._1, s._2("b")))
    dim3_rank.sortBy(_._2, false).take(10).foreach(println)

/*
    //generate 3-dim
    val dim2_table = sc.broadcast(dim2.collectAsMap)
    val dim3_rank = dim2.map{
        case(uid, uList) =>
            var a = Set[Any]();
            uList.foreach{fid =>
                 val b = dim1_table.value.get(fid).getOrElse(Set[Any]())
                 a ++= b
            }
            a --= dim2_table.value.get(uid)
            a --= dim1_table.value.get(uid)
            a --= Set(uid)
            (uid, a.size)
    }
    //dim3.take(10).foreach(println)
    //val dim3_rank = dim3.map(s => (s._1, s._2.size))
    dim3_rank.sortBy(_._2, false).take(10).foreach(println)
*/
    }
}

