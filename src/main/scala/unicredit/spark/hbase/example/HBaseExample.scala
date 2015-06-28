package unicredit.spark.hbase.example

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase._

import scala.collection.immutable.HashMap

/**
 * Created by root on 6/24/15.
 */

object HBaseExample{
  def main(args: Array[String]): Unit = {
    val master = args.filter(x => x.contains("master"))
    val addr = master(0).toString.split("=")(1)
    val sparkConf = new SparkConf().setAppName("SparkWithHBase")
    sparkConf.setMaster(addr)
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    // Add local HBase conf
    conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"))
    implicit val config = HBaseConfig(conf)

    // write example
    // (String, Map[String, String]): (rowID, Map[Columns, Values])
    val tableName = "ds_163_blog"
    val cf = "dcf"
    val rdd: RDD[(String, Map[String, Int])] = sc.parallelize(1 to 100).map(i => (i.toString, HashMap(i.toString -> i)))
    rdd.collect().foreach(println)
    rdd.tohbase(tableName, cf)

    // read example
    // (String, Map[String, Map[String, A]])
    val cfs = Set("dcf")
    val rddRes = sc.hbase[Int](tableName, cfs)
//    println(rddRes.take(5).mkString(","))
    rddRes.collect().foreach(println(_))
//    rddRes.collect()
    println("this is the end of this program!")
  }
}
