package unicredit.spark.hbase.example

import java.io.Serializable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.filter.FilterWrapper.FilterRowRetCode
import org.apache.hadoop.hbase.filter.{BinaryComparator, RowFilter, CompareFilter, PrefixFilter}
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
    val rdd: RDD[(String, Map[String, Int])] = sc.parallelize(1 to 100).map(i => ("%04d".format(i), Map(i.toString -> i)))
    val rddW2: RDD[(String, Map[String, String])] = sc.parallelize(1 to 30).map(i => ("%04d".format(i), HashMap("Col1" -> i.toString)))
    val rddw3: RDD[(String, Map[String, Map[String, String]])] = sc.parallelize(1 to 60).map(i => ("%04d".format(i), Map("others" -> Map("Col2" -> (i*i).toString))))
    rddw3.toHBase(tableName)
    rddW2.toHBase(tableName, cf)
    rdd.collect().foreach(println)
    rdd.toHBase(tableName, cf)
/***********************************
    // read example
    // (String, Map[String, Map[String, A]])
    val cfs = Set("dcf")
    val rddRes = sc.hbase[Int](tableName, cfs)
      .map({ case (k, v) =>
      val cf1 = v("dcf")
      val col1 = cf1("col1")

      List(k, col1) mkString "\t"
    })
//      .saveAsTextFile("test1-output")
//    println(rddRes.take(5).mkString(","))
//    rddRes.collect().foreach(println(_))
//    rddRes.collect()
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    // read test2
    val columns = Map(
      "dcf" -> Set("Col1", "Col3"),
      "others" -> Set("Col2")
    )
    val rddTest2 = sc.hbase[String](tableName, columns)
      .map({ case (k, v) =>
      val cf1 = v("dcf")
      val col1 = cf1("col1")
      val col2 = v("others")("col2")

      List(k, col1, col2) mkString "\t"
    })
//      .saveAsTextFile("test2-output")
//    rddTest2.collect().foreach(println)

    // read test3
    val rddTest3 = sc.withStartRow("0030").withEndRow("0040").hbase[String](tableName, columns)

//    rddTest3.collect().foreach(println)

    // filter test
    val filter = new PrefixFilter(Bytes.toBytes("abc"))
    val families = Set("dcf", "others")
    val rddTest4 = sc.hbase[String](tableName, families, filter)
      .map({ case (k, v) =>
      val cf1 = v("dcf")
      val col1 = cf1("col1")
      val col2 = v("others")("col2")

      List(k, col1, col2) mkString "\t"
    }).saveAsTextFile("test.out")
//    rddTest4.collect().foreach(println)
  *********************/
    val filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("0020")))
    val rddRead = sc.hbase[String](tableName, Set("dcf", "others"), filter)
    //rddRead.collect().foreach(println)
    println(s"this is the end of this program!'${rddRead.count()}'")
  }
}
