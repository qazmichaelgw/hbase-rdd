package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.{Delete, HTable, HBaseAdmin}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import unicredit.spark.hbase._

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {
  val DEFAULT_CONF_PATH = "resources/hbase-site.xml"
  val DEFAULT_CONF = new HBaseConfiguration
  DEFAULT_CONF.addResource(new Path(DEFAULT_CONF_PATH))

  def getConf(confPath: String = null): HBaseConfiguration = {
    if (confPath == null) {
      DEFAULT_CONF
    } else {
      val conf =  new HBaseConfiguration
      conf.addResource(new Path(confPath))
      conf
    }
  }

  def toBytes(value: Any): Array[Byte] = {
    if (value == null)
      return null
    if (value.isInstanceOf[Int])
      return Bytes.toBytes(value.asInstanceOf[Int])
    if (value.isInstanceOf[Long])
      return Bytes.toBytes(value.asInstanceOf[Long])
    if (value.isInstanceOf[Float])
      return Bytes.toBytes(value.asInstanceOf[Float])
    if (value.isInstanceOf[Double])
      return Bytes.toBytes(value.asInstanceOf[Double])

    return Bytes.toBytes(value.toString)
  }

  def drop(tableName: String)(implicit config: HBaseConfig): Unit = {
    val hAdmin = new HBaseAdmin(config.get)
    hAdmin.disableTable(tableName)
    hAdmin.deleteTable(tableName)
  }

  def disable(tableName: String)(implicit config: HBaseConfig): Unit = {
    val hAdmin = new HBaseAdmin(config.get)
    hAdmin.disableTable(tableName)
  }


  def enable(tableName: String)(implicit config: HBaseConfig): Unit = {
    val hAdmin = new HBaseAdmin(config.get)
    hAdmin.enableTable(tableName)
  }

  def recreate(tableName: String)(implicit config: HBaseConfig): Unit = {
    val hAdmin = new HBaseAdmin(config.get)
    val descriptor = hAdmin.getTableDescriptor(Bytes.toBytes(tableName))
    drop(tableName)
    hAdmin.createTable(descriptor)
  }

  def deleteColumn(tableName: String, familyName: String)(implicit config: HBaseConfig): Unit = {
    val hAdmin = new HBaseAdmin(config.get)
    hAdmin.disableTable(tableName)
    hAdmin.deleteColumn(tableName, familyName)
    hAdmin.enableTable(tableName)
  }
  /**
   * Checks if table exists, and requires that it contains the desired column family
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(tableName: String, cFamily: String)(implicit config: HBaseConfig): Boolean = {
    val admin = new HBaseAdmin(config.get)
    if (admin.tableExists(tableName)) {
      val families = admin.getTableDescriptor(tableName.getBytes).getFamiliesKeys
      require(families.contains(cFamily.getBytes), s"Table [$tableName] exists but column family [$cFamily] is missing")
      true
    } else false
  }

  /**
   * Takes a snapshot of the table, the snapshot's name has format "tableName_yyyyMMddHHmmss"
   *
   * @param tableName name of the table
   */
  def snapshot(tableName: String)(implicit config: HBaseConfig): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val suffix = sdf.format(Calendar.getInstance().getTime)
    snapshot(tableName, s"${tableName}_$suffix")
  }

  /**
   * Takes a snapshot of the table
   *
   * @param tableName name of the table
   * @param snapshotName name of the snapshot
   */
  def snapshot(tableName: String, snapshotName: String)(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val tName = tableDescriptor.getTableName
    admin.snapshot(snapshotName, tName)
  }

  /**
   * Creates a table with a column family and made of one or more regions
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(tableName: String, cFamily: String, splitKeys: Seq[String])(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    tableDescriptor.addFamily(new HColumnDescriptor(cFamily))
    if (splitKeys.isEmpty)
      admin.createTable(tableDescriptor)
    else {
      val splitKeysBytes = splitKeys.map(Bytes.toBytes).toArray
      admin.createTable(tableDescriptor, splitKeysBytes)
    }
  }

  /**
   * Given a RDD of keys and the number of requested table's regions, returns an array
   * of keys that are start keys of the table's regions. The array length is
   * ''regionsCount-1'' since the start key of the first region is not needed
   * (since it does not determine a split)
   *
   * @param rdd RDD of strings representing row keys
   * @param regionsCount number of regions
   *
   * @return a sorted sequence of start keys
   */
  def computeSplits(rdd: RDD[String], regionsCount: Int): Seq[String] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}