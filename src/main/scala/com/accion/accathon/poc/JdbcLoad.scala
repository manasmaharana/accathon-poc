package com.accion.accathon.poc
/*
@Author Manas Maharana
Description: Read source data, then transfered/aggregate/ETL them and finally load to target DB using JDBC Connector.
 */
import java.util.Properties
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object JdbcLoad {
  
  val APP_NAME: String = "JdbcLoad"
  val logger = LoggerFactory.getLogger(getClass.getName)
  var CONFIG_CONN: Config = null
  val CONFIG_CONNECTION: String = "env-connection-variables"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.error("No Environment type or Network ID are provided as a argument while running spark job")
      System.exit(1)
    } else {
      val envConfig = args(0)
      CONFIG_CONN = envConfig match {
        case "dev"  => ConfigFactory.load("dev.conf")
        case _      => ConfigFactory.empty()
      }
    }
    
    val NETWORK_ID = Some(args(1))
    val DATABASE_SERVER_NAME = Some(CONFIG_CONN.getString(s"${CONFIG_CONNECTION}.database-server-name"))
    val DATABASE_USERNAME = Some(CONFIG_CONN.getString(s"${CONFIG_CONNECTION}.database-username"))
    val DATABASE_PASSWORD = Some(CONFIG_CONN.getString(s"${CONFIG_CONNECTION}.database-password"))
    val DATABASE_JDBC_PORT = Some(CONFIG_CONN.getString(s"${CONFIG_CONNECTION}.database-jdbc-port"))
    val TYMETRIX360_SCHEMA = Some(CONFIG_CONN.getString(s"${CONFIG_CONNECTION}.tymetrix360-schema"))
    
    val MATTER_LAST_UPDATE = "MATTER_LAST_UPDATE"
    
    val spark = SparkSession.builder().appName(APP_NAME).getOrCreate()
    import spark.implicits._

    // set client schema connection to load data
    val CLIENT_SCHEMA = NETWORK_ID.get.toInt match {
      case 329 => Some("ReportingNetwork_329_Rohit")
      case 241 => Some("Microsoft_Corp_POC")
      case 3   => Some("ReportingMonaNetwork_3_copy")
    }
    // JDBC connection URL to PhenixApp and TyMetrix360 DB
    val tyMetrix360JdbcURL = s"jdbc:sqlserver://${DATABASE_SERVER_NAME.get}:${DATABASE_JDBC_PORT.get};database=${TYMETRIX360_SCHEMA.get}"
    val clientjdbcURL = s"jdbc:sqlserver://${DATABASE_SERVER_NAME.get}:${DATABASE_JDBC_PORT.get};database=${CLIENT_SCHEMA.get}"
    // Server connection
    val connectionProperties = new Properties()
    connectionProperties.put("user", DATABASE_USERNAME.get)
    connectionProperties.put("password", DATABASE_PASSWORD.get)
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    // get client source table data
    val sourceDataDF = (spark.read.jdbc(
      url = tyMetrix360JdbcURL,
      table = MATTER_LAST_UPDATE,
      columnName = "matter_last_update_id",
      lowerBound = 1L,
      upperBound = 100000L,
      numPartitions = 200,
      connectionProperties = connectionProperties)) // select columns those required for target table
      
    //TODO - transfer/aggregation/ETL logics goes here
    val targetDF = sourceDataDF.select(col("matter_last_update_id"), col("matter_id"), col("last_update_time"), col("bulk_query_id"), col("network_id"))
    
    // finally load data to target table
    targetDF.orderBy(col("matter_last_update_id")).write.mode(SaveMode.Append).jdbc(clientjdbcURL, MATTER_LAST_UPDATE, connectionProperties)
  }
}