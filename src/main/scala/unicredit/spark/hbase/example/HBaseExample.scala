package unicredit.spark.hbase.example

import java.io.Serializable

import com.framework.db.hbase.mapping.CustomSerializer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.filter.FilterWrapper.FilterRowRetCode
import org.apache.hadoop.hbase.filter._
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
    val rdd: RDD[(Any, Map[String, Map[String, String]])] = sc.parallelize(1 to 60).map(i => (i*1000, Map("others" -> Map("Col2" -> i.toString), "dcf" -> Map("Col3" -> (i*2).toString))))
    rdd.toHBase(tableName)
    val rdd1: RDD[(Any, Map[String, Int])] = sc.parallelize(1 to 100).map(i => ("Int%04d".format(i), Map(i.toString -> i)))
    rdd1.toHBase(tableName, cf)
//    val tableName = "tv_raw_note_suning"
    /*
    val cf = "dcf"
    val rdd: RDD[(String, Map[String, Int])] = sc.parallelize(1 to 100).map(i => ("%04d".format(i), Map(i.toString -> i)))
    val rddW2: RDD[(String, Map[String, String])] = sc.parallelize(1 to 30).map(i => ("%04d".format(i), HashMap("Col1" -> i.toString)))
    val rddw3: RDD[(String, Map[String, Map[String, String]])] = sc.parallelize(1 to 60).map(i => ("%04d".format(i), Map("others" -> Map("Col2" -> (i*i).toString), "dcf" -> Map("Col3" -> (i*2).toString))))
    val rddW4: RDD[(String, Map[String, String])] = sc.parallelize(1 to 30).map(i => ("abc%04d".format(i), HashMap("Col1" -> i.toString)))
    rddw3.toHBase(tableName)
    rddW2.toHBase(tableName, cf)
    rdd.toHBase(tableName, cf)
    rddW4.toHBase(tableName, cf)
    val headers: Seq[String] = Seq("c1", "c2", "c3")
    val rddFixed: RDD[(String, Seq[String])] = sc.parallelize(1 to 30).map(i => ("fix%04d".format(i), Seq(i.toString, (i+1).toString, (i+2).toString)))
    rddFixed.toHBase(tableName, cf, headers)
 */
    // read example
    // case 1 one cf all columns
//    val families = Set("dcf")
//    val rddCase1 = sc.hbase[String](tableName, families)
//    rddCase1.collect().foreach(println)
//    println("\n##############################################################\n")
//    // case 2 multi-cf all columns
//    val cfs2 = Set("dcf", "others")
//    val rddCase2 = sc.hbase[String](tableName, cfs2)
//    rddCase2.collect().foreach(println)
//    println("\n##############################################################\n")
//    // case 3 nested map
//    val columns = Map(
//      "dcf" -> Set("Col1", "Col3"),
//      "others" -> Set("Col2")
//    )
//    val rddCase3 = sc.hbase[String](tableName, columns)
//    rddCase3.collect().foreach(println)
//    println("\n##############################################################\n")
//    // case 4 filter
//    val rddCase4 = sc.withStartRow("0020").withEndRow("0069").hbase[String](tableName, Set("dcf", "others"))
////    rddReaad.saveAsTextFile("rowfilterTest")
//    rddCase4.collect().foreach(println)
//    println("\n##############################################################\n")
//    // case 5 prefix filter
//    val filter = new PrefixFilter(Bytes.toBytes("abc"))
//    val rddCase5 = sc.hbase[String](tableName, Set("dcf", "others"), filter)
//    //    rddReaad.saveAsTextFile("rowfilterTest")
//    rddCase5.collect().foreach(println)
//    println("\n##############################################################\n")


    val families = Set("dcf", "others")
    val rddCase1 = sc.hbase[Object](tableName, families)
    rddCase1.collect().foreach {
      case (k, v) => println(Bytes.toString(k) + " : " + v)
    }

    println("\n##############################################################\n")

    val rks = List("Int%04d".format(2), "Int%04d".format(3))
    val rddTest = sc.query(tableName, rks)
    rddTest.collect().foreach {
      case (k, v) => println(Bytes.toString(k) + " : " + v)
    }
    println("\n##############################################################\n")
    val rddTest1 = sc.queryWithFamily(tableName, rks)
    rddTest1.collect().foreach {
      case (k, v) => println(Bytes.toString(k) + " : " + v)
    }

    sc.delete(tableName, "Int%04d".format(2))
    val rddAfter1 = sc.hbase[Object](tableName, families)
    rddAfter1.collect().foreach {
      case (k, v) => println(Bytes.toString(k) + " : " + v)
    }
    println("\n##############################################################\n")
    println(s"this is the end of this program!")
  }
}
