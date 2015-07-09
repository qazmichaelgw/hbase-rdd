/* Copyright 2014 UniCredit S.p.A.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package spark.hbase

import com.framework.db.hbase.mapping.{CellType, CustomSerializer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Delete, Put, HBaseAdmin}
import org.apache.hadoop.hbase.{ HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes

//import org.json4s._
//import org.json4s.jackson.JsonMethods._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Adds implicit methods to `RDD[(String, Map[String, A])]`,
 * `RDD[(String, Seq[A])]` and
 * `RDD[(String, Map[String, Map[String, A]])]`
 * to write to HBase sources.
 */
trait HBaseWriteSupport{
//  @transient val cs = new CustomSerializer()
  type PutAdder[A] = (Put, Array[Byte], Array[Byte], A) => Unit

  implicit def toHBaseRDDSimple[A](rdd: RDD[(Any, Map[String, A])])(implicit writer: Writes[A]): HBaseRDDSimple[A] =
    new HBaseRDDSimple(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: A) => put.add(cf, q, writer.write(v))})

  implicit def toHBaseRDDSimpleT[A](rdd: RDD[(Any, Map[String, (A, Long)])])(implicit writer: Writes[A]): HBaseRDDSimple[(A, Long)] =
    new HBaseRDDSimple(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: (A, Long)) => put.add(cf, q, v._2, writer.write(v._1))})

  implicit def toHBaseRDDFixed[A](rdd: RDD[(Any, Seq[A])])(implicit writer: Writes[A]): HBaseRDDFixed[A] =
    new HBaseRDDFixed(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: A) => put.add(cf, q, writer.write(v))})

  implicit def toHBaseRDDFixedT[A](rdd: RDD[(Any, Seq[(A, Long)])])(implicit writer: Writes[A]): HBaseRDDFixed[(A, Long)] =
    new HBaseRDDFixed(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: (A, Long)) => put.add(cf, q, v._2, writer.write(v._1))})

  implicit def toHBaseRDD[A](rdd: RDD[(Any, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HBaseRDD[A] =
    new HBaseRDD(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: A) => put.add(cf, q, writer.write(v))})

  implicit def toHBaseRDDT[A](rdd: RDD[(Any, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HBaseRDD[(A, Long)] =
    new HBaseRDD(rdd, { (put: Put, cf: Array[Byte], q: Array[Byte], v: (A, Long)) => put.add(cf, q, v._2, writer.write(v._1))})

  implicit val byteArrayWriter = new Writes[Array[Byte]] {
    def write(data: Array[Byte]) = data
  }

  implicit val shortWriter = new Writes[Short] {
    def write(data: Short) = {
      Array(CellType.Short.getValue, 0).map(_.toByte)  ++ toBytes(data)
    }
  }

  implicit val intWriter = new Writes[Int] {
    def write(data: Int) = {
      Array(CellType.Integer.getValue, 0).map(_.toByte)  ++ toBytes(data)
    }
  }

  implicit val stringWriter = new Writes[String] {
    def write(data: String) = {
      Array(CellType.String.getValue, 0).map(_.toByte) ++ toBytes(data)
    }
  }

//  implicit val jsonWriter = new Writes[JValue] {
//    def write(data: JValue) = compact(data).getBytes
//  }

//  implicit val objWriter = new Writes[Object] {
//    def write(data: Object) = {
//      cs.obj2Byte(data)
//    }
//  }

//  implicit val intWriter = new Writes[Int] {
//    def write(data: Int) = {
//      var value = data
//      val b = new Array[scala.Byte](4)
//      for (i <- 3 to (1, -1)) {
//        b(i) = value.byteValue
//        value = value >>> 8
//      }
//      b(0) = value.byteValue
//      b
//    }
//  }
}

sealed abstract class HBaseWriteHelpers[A] {
  protected def convert(id: Any, values: Map[String, Map[String, A]], put: PutAdder[A]) = {
    def bytes(s: String) = Bytes.toBytes(s)

    val p = new Put(toBytes(id))
    var empty = true
    for {
      (family, content) <- values
      (key, value) <- content
    } {
      empty = false
      put(p, bytes(family), bytes(key), value)
    }

    if (empty) None else Some(new ImmutableBytesWritable, p)
  }

  protected def createTable(table: String, families: List[String], admin: HBaseAdmin) = {
    if (!admin.isTableAvailable(table)) {
      val tableName = TableName.valueOf(table)
      val tableDescriptor = new HTableDescriptor(tableName)

      families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
      admin.createTable(tableDescriptor)
    }
  }

  protected def createJob(table: String, conf: Configuration) = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }
}

final class HBaseRDDSimple[A](val rdd: RDD[(Any, Map[String, A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, A])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def toHBase(table: String, family: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)
    createTable(table, List(family), new HBaseAdmin(conf))

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseRDDFixed[A](val rdd: RDD[(Any, Seq[A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family, and columns are fixed, so that their names can be passed as a sequence.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Seq[A])]`,
   * where the first value is the rowkey and the second is a sequence of values
   * that are associated to a sequence of headers.
   */
  def toHBase(table: String, family: String, headers: Seq[String])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)
    createTable(table, List(family), new HBaseAdmin(conf))

    val sc = rdd.context
    val bheaders = sc.broadcast(headers)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> Map(bheaders.value zip v: _*)), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseRDD[A](val rdd: RDD[(Any, Map[String, Map[String, A]])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, Map[String, A]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def toHBase(table: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}