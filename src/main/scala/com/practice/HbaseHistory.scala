package com.practice

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.util.control._
import util.control.Breaks._

object HbaseHistory {
  println("Scala Example with Hbase read and update, also delete old row.....")

  // printRow func
  def printRow(result : Result) = {
    val cells = result.rawCells();
    print( Bytes.toString(result.getRow) + " : " )
    println(cells.length)

    for(i <- 0 to cells.length-1) {
      val cf_name = Bytes.toString(CellUtil.cloneFamily(cells{i}))
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cells{i}))
      val col_value = Bytes.toString(CellUtil.cloneValue(cells{i}))
      print("(%s,%s,%s) ".format(cf_name, col_name, col_value))
    }

    println(cells{0})
  }

  // Add new Row and delete old of specific hbase table
  def updateRow(result : Result, table: Table) = {
    val cells = result.rawCells();
    println( "row key is " + Bytes.toString(result.getRow) )

    val loop = new Breaks;

    breakable {

      for (i <- 0 to cells.length - 1) {
        val cf_name = Bytes.toString(CellUtil.cloneFamily(cells {i}))
        var col_name = Bytes.toString(CellUtil.cloneQualifier(cells {i}))
        val col_value = Bytes.toString(CellUtil.cloneValue(cells {i}))

        // required column family to be updated
        if ( (col_name.indexOf("prem:") == 0) && !(col_name.contains("_")) ){
          println("(%s,%s,%s) ".format(cf_name, col_name, col_value))

          var splitArray = col_name.split(":")
          var tempCol = splitArray {1}.replaceFirst("[0]+", "1500_")
          var new_col_name = splitArray {0} + ":" + tempCol + ":" + splitArray {2}

          // do the put
          var put = new Put(result.getRow)
          put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(new_col_name), Bytes.toBytes(col_value))
          table.put(put)
          println("modified row is (%s,%s,%s) ".format(cf_name, new_col_name, col_value))

          // do the delete
          var delete = new Delete(result.getRow) // get the row key
          delete.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_name))
          table.delete(delete)
        }
      }
    }
     //println(cells{0})
  }


  def main(args: Array[String]){
    val hbaseConf : Configuration = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf( Bytes.toBytes("emp_new") ) )

    println("\nScan of all rows start here...........")

    var scanResult = table.getScanner(new Scan()).iterator()

    while (scanResult.hasNext) {
      updateRow(scanResult.next(), table)
    }

    println("\nScan of all rows end here...........")

    table.close()
    connection.close()
  }


}

