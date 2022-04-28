package org.cusp.bdi.sknn.test

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.sknn.ds.util.SupportedSpatialIndexes
import org.cusp.bdi.sknn.{SparkKnn, SparkKnnKryoRegistrator}

object Test_kNN_Join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    // assumes local mode
    sparkConf.setMaster("local[*]")
             .set("spark.driver.memory", "1G")
             .set("spark.executor.memory", "4G")
             .set("spark.executor.instances", "4")
             .set("spark.executor.cores", "5")

    sparkConf.setAppName(this.getClass.getName)
             .set("spark.serializer", classOf[KryoSerializer].getName)
             .set("spark.kryo.registrator", classOf[SparkKnnKryoRegistrator].getName)

    val sc = new SparkContext(sparkConf)

    def getRDD(fileName: String): RDD[Point] =
      sc.textFile(fileName)
        .mapPartitions(_.map(line => {

          // parse the line and form the spatial object

          val parts = line.split(',')
          val coord = parts
            .map(xyStr => {
              val xy = xyStr.split(' ')

              (xy(0).toDouble.toInt, xy(1).toDouble.toInt)
            })

          (parts(0), coord(0))
        }))
        .filter(_ != null)
        .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val rddLeft = getRDD("firstDataset.csv")
    val rddRight = getRDD("secondDataset.csv")

    SparkKnn(true).knnJoin(rddLeft, rddRight, SupportedSpatialIndexes.QUADTREE, 10, 100, 0)
                  .mapPartitions(_.map(row =>
                    "%s;%s".format(row._1.userData, row._2.map(matchInfo =>
                      "%.8f,%s".format(matchInfo._1, matchInfo._2.userData)).mkString(";"))))
                  .saveAsTextFile("SparkKnn_knnJoin_Results", classOf[GzipCodec])
  }
}
