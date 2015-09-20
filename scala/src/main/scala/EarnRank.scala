package rank

/* EarnRank.scala */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.collection.mutable.{Set, Map}
import java.util._ 
import java.sql.{DriverManager, Connection, Statement}

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {
  val redisHost = "192.168.1.10"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  //lazy val redis = new Jedis(redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}

object EarnRank {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Earn Rank")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var tableName = "entry"
    var url = "jdbc:mysql://192.168.1.10:3306/friend"
    var name = "com.mysql.jdbc.Driver"
    var user = "root"
    var password = ""
    var db = new DBHelper(url, name, user, password)
    var ret = db.st.executeQuery("SELECT MAX(id) maxid FROM "+tableName)
    ret.next()
    var maxid = ret.getLong("maxid")
    db.close()
    println(maxid)

    val prop = new Properties();  
    prop.setProperty("user", user);
    prop.setProperty("password", password);
    val df1 = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", tableName, "id", 1, maxid, conf.getInt("spark.cores.max", 1), prop)

    tableName = "invite_wallet"
    url = "jdbc:mysql://192.168.1.10:3306/friend"
    name = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    db = new DBHelper(url, name, user, password)
    ret = db.st.executeQuery("SELECT MAX(id) maxid FROM "+tableName)
    ret.next()
    maxid = ret.getLong("maxid")
    db.close()
    println(maxid)

    prop.setProperty("user", user);
    prop.setProperty("password", password);
    val df2 = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", tableName, "id", 1, maxid, conf.getInt("spark.cores.max", 1), prop)
    df2.registerTempTable("wallet")
    val earn_table = sqlContext.sql("SELECT Mobile,SUM(Money) earn FROM wallet WHERE Status=1 GROUP BY Mobile")
    //earn_table.show()

    tableName = "friend_bf"
    url = "jdbc:mysql://192.168.1.10:3306/friend"
    name = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    db = new DBHelper(url, name, user, password)
    ret = db.st.executeQuery("SELECT MAX(id) maxid FROM "+tableName)
    ret.next()
    maxid = ret.getLong("maxid")
    db.close()
    println(maxid)

    prop.setProperty("user", user);
    prop.setProperty("password", password);
    val df = sqlContext.read.jdbc("jdbc:mysql://192.168.1.10:3306/friend", tableName, "id", 1, maxid, conf.getInt("spark.cores.max", 1), prop)
    df.registerTempTable("friend")
    val friend_table = sqlContext.sql("SELECT entryUuid,friendId FROM friend WHERE valiableFlag=1")

    val friend_earn = friend_table.join(df1, $"entryUuid" === $"UUID", "inner").selectExpr("entryMobile as uMobile","friendId").join(df1, $"friendId" === $"UUID", "inner").join(earn_table, $"entryMobile" === $"Mobile", "inner").select("uMobile","entryMobile","earn").as("friend_earn")

    friend_earn.map(s => (s(0), (s(1), s(2).toString.toInt))).groupByKey().foreach(s => {
        val redis = RedisClient.pool.getResource
        //s._2.toSeq.sortWith(_._2>_._2 ).take(10).foreach(a => {
        s._2.foreach(a => {
            redis.zadd(s._1.toString, a._2, a._1.toString)
        })
        RedisClient.pool.returnResource(redis)
    })
    }
}

