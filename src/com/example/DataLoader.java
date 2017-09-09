package main.scala

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import com.datastax.driver.core.LocalDate
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{functions => Functions}

/*
 *  create table stocks(date date,open double,high double,low double,close double, 
 *  volume double,adjclose double,symbol text, primary key (date, symbol));
 * 
 * */

case class Stock(date:Timestamp,open:Double,high:Double,
    low:Double,close:Double,volume:Double,adjclose:Double,symbol:String)

object DataLoader {
  
  private val logger = LoggerFactory.getLogger(getClass)

  def loadAs(file: String, table: String, cache: Boolean = false) {
    val options = Map("header" -> "true","inferSchema" -> "true")
    spark.read.format("csv").options(options).load(file).createOrReplaceTempView(table)
    if (cache) {
      spark.table(table).cache()
    }
  }
  
  def saveEachPartition(rows:Iterator[Stock]) = {
    val cassandraConnector = CassandraConnector.apply(sparkConf)
    val cassandraSession = cassandraConnector.openSession() 
    val statement = cassandraSession.prepare("insert into demo.stocks (date,open,high,low,close,volume,adjclose,symbol) values (?,?,?,?,?,?,?,?)")
    
    val startTime = System.currentTimeMillis()
   
    val results = rows.toArray.map{row => 
        val localDate = LocalDate.fromMillisSinceEpoch(row.date.getTime)
        
        val boundStatement = statement.bind()
        boundStatement.setDate("date", localDate)
        boundStatement.setString("symbol", row.symbol)
        boundStatement.setDouble("open", row.open)
        boundStatement.setDouble("high", row.high)
        boundStatement.setDouble("close", row.close)
        boundStatement.setDouble("low", row.low)
        boundStatement.setDouble("volume", row.volume)
        boundStatement.setDouble("adjclose", row.adjclose)
        
        val rs = cassandraSession.execute(boundStatement)
        (row.date, row.symbol, rs.wasApplied())
    }
    val elapsed = System.currentTimeMillis() - startTime
    logger.warn(s"Partition size: ${results.length}, taken: $elapsed ms")
    cassandraSession.close()
    results.toIterator
  }
  
  def toStock(row:Row) = {
    Stock(row.getTimestamp(0)
         , row.getDouble(1)
         , row.getDouble(2)
         , row.getDouble(3)
         , row.getDouble(4)
         , row.getDouble(5)
         , row.getDouble(6)
         , row.getString(7))
  }

  

  def main(args: Array[String]) {
    
    val numPartitions = 10
    
    println("Spark Web UI: " + spark.sparkContext.uiWebUrl)
    loadAs("/home/training/Downloads/datasets/stocks.csv", "stocks")
    
    import spark.implicits._
    val stocks = spark.table("stocks").map(toStock).coalesce(numPartitions) //.repartition(Functions.col("date"))
    stocks.select("date").distinct().show()

    val results = stocks.mapPartitions(saveEachPartition).toDF("date", "symbol", "applied")
    results.show()
    results.coalesce(1).write.mode(SaveMode.Overwrite).csv("save-results")
    spark.close()
  }

}
