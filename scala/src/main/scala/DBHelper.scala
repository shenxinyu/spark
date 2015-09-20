package rank

import java.sql.{DriverManager, Connection, Statement}

class DBHelper(l :String, n: String, u :String, p :String) {  
    val url = l
    val name = n
    val user = u
    val password = p
 
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
