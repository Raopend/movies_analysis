package util
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.QueryRunner


object JDBCUtil {
  private val dataSource = new ComboPooledDataSource()

  private val user = "root"
  private val password = "123456"
  private val url = "jdbc:mysql://localhost:3306/movies"

  dataSource.setUser(user)
  dataSource.setPassword(password)
  dataSource.setDriverClass("com.mysql.jdbc.Driver")
  dataSource.setJdbcUrl(url)
  dataSource.setAutoCommitOnClose(false)


  def getQueryRunner(): Option[QueryRunner]={
    try {
      Some(new QueryRunner(dataSource))
    }catch {
      case e:Exception =>
        e.printStackTrace()
        None
    }
  }
}