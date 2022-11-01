package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JdbcProvisioner {

  def main(args: Array[String]) {
    val IpServer = "34.88.125.105"

    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "postgres"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

      // Metadata de usuarios
      println("Creando la tabla PROYECTO_user_metadata (id TEXT, name TEXT, email TEXT, quota BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_user_metadata (id TEXT, name TEXT, email TEXT, quota BIGINT)")

      // Total de bytes recibidos por antena.
      println("Creando la tabla PROYECTO_streamingBytesPorAntena (antenna_id TEXT, sum_bytes BIGINT, data TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_streamingBytesPorAntena (antenna_id TEXT, sum_bytes BIGINT, data TIMESTAMP)")

      // Total bytes transmitidos por id de usuario
      println("Creando la tabla PROYECTO_streamingBytesPorUsuario (id TEXT, sum_bytes BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_streamingBytesPorUsuario (id TEXT, sum_bytes BIGINT, date TIMESTAMP)")

      // Total de bytes transmitidos por aplicación
      println("Creando la tabla PROYECTO_streamingBytesPorAplicacion (app TEXT, sum_bytes BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_streamingBytesPorAplicacion (app TEXT, sum_bytes BIGINT, date TIMESTAMP)")

      // Total de bytes recibidos por antena.
      println("Creando la tabla PROYECTO_batchBytesPorAntena (antenna_id STRING, sum_bytes BIGINT, data TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_batchBytesPorAntena (antenna_id TEXT, sum_bytes BIGINT, data TIMESTAMP)")

      // Total bytes transmitidos por id de usuario
      println("Creando la tabla PROYECTO_batchBytesPorUsuario (id TEXT, sum_bytes BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_batchBytesPorUsuario (id TEXT, sum_bytes BIGINT, date TIMESTAMP)")

      // Total de bytes transmitidos por aplicación
      println("Creando la tabla PROYECTO_batchBytesPorAplicacion (app TEXT, sum_bytes BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_batchBytesPorAplicacion (app TEXT, sum_bytes BIGINT, date TIMESTAMP)")

      // email de usuarios que han sobrepasado la cuota por hora
      println("Creando la tabla PROYECTO_batchUsersOverLimit (id TEXT, sum_bytes BIGINT, quota BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS PROYECTO_batchUsersOverLimit (id TEXT, sum_bytes BIGINT, quota BIGINT, date TIMESTAMP)")

      println("Dando de alta la información de usuarios")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
      statement.execute("INSERT INTO PROYECTO_user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")

    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
