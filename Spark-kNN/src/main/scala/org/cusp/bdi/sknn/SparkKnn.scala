package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.sknn.ds.util.SpatialIdxOperations
import org.cusp.bdi.sknn.util.Helper
//import org.cusp.bdi.sknn.SparkKnn.{DEFAULT_SPARK_MEM_FRACTION, DEFAULT_SPARK_STORAGE_MEMORY_FRACTION, RESERVED_SPARK_MEMORY, fComputeGridXY_Coord, fComputeGridXY_Point}
import org.cusp.bdi.sknn.SparkKnn.{DEFAULT_SPARK_MEM_FRACTION, DEFAULT_SPARK_STORAGE_MEMORY_FRACTION, RESERVED_SPARK_MEMORY, fComputeGridXY_Coord, fComputeGridXY_Point}
import org.cusp.bdi.sknn.ds.util.SpatialIdxOperations.{fCastToPartitionerPointUserData, fSpatialIndexInit}
import org.cusp.bdi.sknn.ds.util.{PartitionerPointUserData, SupportedSpatialIndexes}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SparkKnn extends Serializable {

  // Fraction of (heap space - 300MB) used for execution and storage
  // As per Spark 2.4.0 configuration
  val DEFAULT_SPARK_MEM_FRACTION: Double = 0.60

  // Amount of storage memory immune to eviction expressed as a fraction of the size of the region set aside by spark.memory.fraction
  // As per Spark 2.4.0 configuration
  val DEFAULT_SPARK_STORAGE_MEMORY_FRACTION: Double = 0.50

  // Memory reserved by Spark (300MB). The value must not change and is hardcoded into Spark since 1.6.0
  val RESERVED_SPARK_MEMORY = 3e8

  def fComputeGridXY_Point: (Point, Int) => (Int, Int) = (point: Point, gridDim: Int) => fComputeGridXY_Coord(point.x, point.y, gridDim)

  def fComputeGridXY_Coord: (Double, Double, Int) => (Int, Int) = (x: Double, y: Double, gridDim: Int) => (Helper.round(x / gridDim), Helper.round(y / gridDim))
}

case class SparkKnn(debugMode: Boolean) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point], spatialIndexType: SupportedSpatialIndexes.Value, k: Int, initialGridDim: Int, partitionMaxByteSize: Long): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin")

    val (partitioner_ActiveRight, queueMBR_ActiveRight, gridDimRight) = computeCapacity(rddRight, rddLeft, spatialIndexType, k, initialGridDim, partitionMaxByteSize, isAllKNN = false)

    knnJoinExecute(rddLeft, rddRight, partitioner_ActiveRight, queueMBR_ActiveRight, gridDimRight, spatialIndexType, k)
  }

  def allKnnJoin(rddLeft: RDD[Point], rddRight: RDD[Point], spatialIndexType: SupportedSpatialIndexes.Value, k: Int, initialGridDim: Int, partitionMaxByteSize: Long): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partitioner_ActiveRight, queueMBR_ActiveRight, gridDimRight) = computeCapacity(rddRight, rddLeft, spatialIndexType, k, initialGridDim, partitionMaxByteSize, isAllKNN = true)
    val (partitioner_ActiveLeft, qMBR_ActiveLeft, gridDimLeft) = computeCapacity(rddLeft, rddRight, spatialIndexType, k, initialGridDim, partitionMaxByteSize, isAllKNN = true)

    knnJoinExecute(rddRight, rddLeft, partitioner_ActiveLeft, qMBR_ActiveLeft, gridDimLeft, spatialIndexType, k)
      .union(knnJoinExecute(rddLeft, rddRight, partitioner_ActiveRight, queueMBR_ActiveRight, gridDimRight, spatialIndexType, k))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point], //pointQuery: Point, // pass one of the two, one must be !null
                             rddActiveRight: RDD[Point], partitioner_ActiveRight: SpatialIndex, queueMBR_ActiveRight: mutable.Queue[MBR], gridDim_ActiveRight: Int,
                             spatialIndexType: SupportedSpatialIndexes.Value, k: Int): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute")

    if (rddActiveLeft == null)
      throw new Exception("Only rddActiveLeft or pointQuery is allowed to be !null")

    val bvPartitioner_ActiveRight: Broadcast[SpatialIndex] = rddActiveRight.context.broadcast(partitioner_ActiveRight)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>" + queueMBR_ActiveRight.mkString("\n\t>>"))

    val startTime = System.currentTimeMillis

    val (setNeededParts, numRounds) =
      rddActiveLeft
        .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDim_ActiveRight), null)))
        .reduceByKey((_, _) => null) // distinct
        .mapPartitions(_.map(row => {

          val arrPartitionId = SpatialIdxOperations.extractLstPartition(bvPartitioner_ActiveRight.value, row._1, k)

          (arrPartitionId.toSet, arrPartitionId.length)
        }))
        .treeReduce((row1, row2) => (row1._1 ++ row2._1, Helper.max(row1._2, row2._2)))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>setNeededParts: [%s]".format(setNeededParts.mkString(",")))

    var actualNumberOfPartitions = queueMBR_ActiveRight.length

    val mapPartMBRs: Map[Int, MBR] =
      (if (setNeededParts.size < actualNumberOfPartitions) { // prune

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>setNeededParts.size < actualNumberOfPartitions: %,d <= %,d"
          .format(setNeededParts.size, actualNumberOfPartitions))

        actualNumberOfPartitions = -1

        // keep the original partition mapping (Partitioner cannot update at this point)
        // but shift according to the pruned partitions
        val mapRet = queueMBR_ActiveRight
          .map(mbr =>
            if (setNeededParts.contains(mbr.assignedPartition)) {

              val origAssignPart = mbr.assignedPartition

              actualNumberOfPartitions += 1

              mbr.assignedPartition = actualNumberOfPartitions

              (origAssignPart, mbr)
            }
            else
              null
          )
          .filter(_ != null)

        actualNumberOfPartitions += 1
        mapRet
      }
      else
        queueMBR_ActiveRight.map(mbr => (mbr.assignedPartition, mbr))
        ).toMap

    queueMBR_ActiveRight.clear() // not needed anymore, GC its objects

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d LeftDS numRounds done. numRounds: %,d time in %,d MS mapPartMBRs: %s "
      .format(actualNumberOfPartitions, numRounds, System.currentTimeMillis - startTime, mapPartMBRs.mkString(",")))

    // build a spatial index on each partition
    val rddSpIdx: RDD[(Int, AnyRef)] = rddActiveRight
      .mapPartitions(_.map(point => {

        val gridXY = fComputeGridXY_Point(point, gridDim_ActiveRight)

        val partitionIdx = fCastToPartitionerPointUserData(bvPartitioner_ActiveRight.value.findExact(gridXY._1, gridXY._2)).partitionIdx
        val mbrOpt = mapPartMBRs.get(partitionIdx)

        if (mbrOpt.isEmpty)
          null
        else
          (mbrOpt.get.assignedPartition, point)
      }))
      .filter(_ != null)
      .partitionBy(new Partitioner() {

        override def numPartitions: Int = actualNumberOfPartitions

        override def getPartition(key: Any): Int =
          key match {
            case pIdx: Int =>
              if (pIdx < 0) -pIdx - 1 else pIdx // -1 to undo the +1 during random assignment of leftRDD (there is no -0)
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis

        val mbrPartition = mapPartMBRs.find(_._2.assignedPartition == pIdx).get._2

        val rectSI = buildRectBounds(mbrPartition.left * gridDim_ActiveRight - gridDim_ActiveRight, mbrPartition.bottom * gridDim_ActiveRight - gridDim_ActiveRight, mbrPartition.right * gridDim_ActiveRight + gridDim_ActiveRight, mbrPartition.top * gridDim_ActiveRight + gridDim_ActiveRight)

        val spatialIndex = fSpatialIndexInit(spatialIndexType)

        spatialIndex.insert(rectSI, iter.map(_._2), gridDim_ActiveRight)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d"
          .format(pIdx, System.currentTimeMillis - startTime, spatialIndex, -1 /*SizeEstimator.estimate(spatialIndex)*/))

        Iterator((mbrPartition.assignedPartition, spatialIndex.asInstanceOf[AnyRef]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    var rddPoint: RDD[(Int, AnyRef)] = rddActiveLeft
      .mapPartitions(iter => {

        val rand = Random

        iter.map(point => {

          val arrPartitionId = SpatialIdxOperations.extractLstPartition(bvPartitioner_ActiveRight.value, fComputeGridXY_Point(point, gridDim_ActiveRight), k)
                                                   .map(pIdx => mapPartMBRs(pIdx).assignedPartition)

          if (arrPartitionId.length < numRounds) {

            arrPartitionId.sizeHint(numRounds)

            while (arrPartitionId.length < numRounds)
              arrPartitionId.insert(rand.nextInt(arrPartitionId.length + 1), -(rand.nextInt(actualNumberOfPartitions) + 1)) // +1 since there is no signed 0, - to indicate a skip partition
          }

          (arrPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), arrPartitionId.tail))
        })
      })

    for (currRoundNum <- 1 to numRounds) {
      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          var counterPoints = 0L
          var counterKNN = 0L

          // first entry is always the spatial index per PartitionerAwareUnionRDD
          val spatialIndex: SpatialIndex = iter.next()._2 match {
            case spIdx: SpatialIndex =>
              spIdx
          }

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                counterPoints += 1

                if (row._1 >= 0) {

                  counterKNN += 1
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
                }

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d numPoints: %,d counterKNN: %,d".format(pIdx, currRoundNum, counterPoints, counterKNN))

                (rowData.nextPartId(), rowData)
            })
        })

      if (currRoundNum == 1)
        bvPartitioner_ActiveRight.unpersist(false)
    }

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.map(node => (Math.sqrt(node.distance), node.data)))
      }))
  }

  /**
   * Stage 1 for constructing a dataset partitioner - Analyzing the input datasets
   *
   * @param rddRight , the right RDD
   * @param rddLeft  , the left RDD
   * @param isAllKNN , true if this is an all kNN operation. The resources will be divided by 2
   * @return a tuple consisting of (1) the partition point capacity range (min-max), (2) the MBR of the right dataset, (3) the adjusted grid width
   */
  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], spatialIndexType: SupportedSpatialIndexes.Value, k: Int, initialGridDim: Int, partitionMaxByteSize: Long, isAllKNN: Boolean): (SpatialIndex, mutable.Queue[MBR], Int) = {

    var gridDim = initialGridDim
    val sparkNumExec: Int = rddRight.context.getConf.get("spark.executor.instances").toInt
    val sparkExecMem: Long = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val sparkExecCores: Int = rddRight.context.getConf.get("spark.executor.cores").toInt
    val countAllCores: Int = sparkNumExec * sparkExecCores

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>sparkNumExec: %,d sparkExecMem: %,d sparkMemFrac: %,.2f sparkExecCores: %,d countAllCores: %,d"
      .format(sparkNumExec, sparkExecMem, DEFAULT_SPARK_MEM_FRACTION, sparkExecCores, countAllCores))

    lazy val startTime = System.currentTimeMillis

    var rddRight_gridXY_Size_MBR_Count: RDD[((Int, Int), (Long, MBR, Long))] = rddRight
      .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDim), (SizeEstimator.estimate(point), new MBR(point.x.toInt, point.y.toInt), 1L))))
      .reduceByKey((tuple1, tuple2) => (Helper.max(tuple1._1, tuple2._1), tuple1._2.merge(tuple2._2), tuple1._3 + tuple2._3)) // gets rid of duplicate grid cells but keeps the MBR, max obj size, and # points in each cell
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Analyze Right RDD
    val (memMaxPointRight, mbrRight, maxObjCountRight, countObjRight) = rddRight_gridXY_Size_MBR_Count
      .mapPartitions(_.map(row => (row._2._1, row._2._2, row._2._3, row._2._3))) // discard box coordinates and introduce overall counter
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), Helper.max(row1._3, row2._3), row1._4 + row2._4)) // aggregate results

    //      rddRight
    //      .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDim), (SizeEstimator.estimate(point), new MBR(point.x.toInt, point.y.toInt), 1L))))
    //      .reduceByKey((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3)) // gets rid of duplicates but keeps track of the actual MBR and count
    //      .mapPartitions(_.map(row => (row._2, row._2._3))) // necessary to introduce total row count
    //      .treeReduce((row1, row2) => ((Helper.max(row1._1._1, row2._1._1), row1._1._2.merge(row2._1._2), Helper.max(row1._1._3, row2._1._3)), row1._2 + row2._2)) // aggregate results

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done memMaxPointRight: %,d mbrRight: %s maxObjCountRight: %,d countObjRight: %,d Time: %,d MS"
      .format(memMaxPointRight, mbrRight, maxObjCountRight, countObjRight, System.currentTimeMillis - startTime))

    // Partitioner MEM estimate
    val memRect = SizeEstimator.estimate(new Rectangle(new Geom2D(), new Geom2D()))
    val memStorageCore = math.ceil(0.5 * (sparkExecMem - RESERVED_SPARK_MEMORY) * DEFAULT_SPARK_MEM_FRACTION * DEFAULT_SPARK_STORAGE_MEMORY_FRACTION / sparkExecCores).toLong
    val memMaxPart: Double = if (partitionMaxByteSize <= 0) memStorageCore else Helper.min(memStorageCore, partitionMaxByteSize)

    //    if (memStorageCore < 1e9 / sparkExecCores)
    //      throw new Exception("The memory left for the core is less than 1GB. Please increase the executor's memory")

    /*
     * Right dataset (Spatial Indexes) size estimate. Every row in the right RDD contains:
     * 1. partition ID (int) <- insignificant here, so it's not accounted for.
     * 2. spatial index: # of nodes with objects
    */
    val spIdxMockRight = fSpatialIndexInit(spatialIndexType)
    val memSpIdxMockRight = SizeEstimator.estimate(spIdxMockRight)
    spIdxMockRight.insert(new Rectangle(new Geom2D(Double.MaxValue / 2)), (0 until spIdxMockRight.nodeCapacity).map(new Point(_)).iterator, 1)
    val memSpIdxInsertOverheadRight = (SizeEstimator.estimate(spIdxMockRight) - memSpIdxMockRight - memRect) / spIdxMockRight.nodeCapacity.toDouble

    val countNodesSpIdxRight = spIdxMockRight.estimateNodeCount(countObjRight)

    val memRightRDD = (countObjRight * memMaxPointRight) + (countNodesSpIdxRight * (memSpIdxMockRight + memRect + memSpIdxInsertOverheadRight)) * (if (isAllKNN) 2 else 1)

    val countPartitionRight = Math.ceil(memRightRDD / memMaxPart).toInt

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS memSpIdxMockRight: %,d memSpIdxInsertOverheadRight: %,.2f memRect: %,d countNodesSpIdxRight: %,d memRightRDD: %,.2f memMaxPart: %,.2f countPartitionRight: %,d"
      .format(memSpIdxMockRight, memSpIdxInsertOverheadRight, memRect, countNodesSpIdxRight, memRightRDD, memMaxPart, countPartitionRight))

    /*
     * every row in the left RDD contains:
     *   1. partition ID (int)
     *   2. point info (RowData -> (point, SortedLinkedList, list of partitions to visit))
     *
     *   point: coordinates + userData <- from raw file
     *   SortedLinkedList: point matches from the right RDD of size up to k
     */
    var countPartitionsLeft = 0.0

    if (rddLeft != null) { // null when knn between RDD and a single Point

      val startTime = System.currentTimeMillis

      val (memMaxPointLeft, countPointLeft) = rddLeft
        .mapPartitions(_.map(point => (SizeEstimator.estimate(point), 1L)))
        .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2 + row2._2))

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS info done memMaxPointLeft: %,d countPointLeft: %,d Time: %,d MS"
        .format(memMaxPointLeft, countPointLeft, System.currentTimeMillis - startTime))

      val arrPartIdMockLeft = ArrayBuffer.fill[Int](Helper.max(countPartitionRight, countAllCores))(0)
      val memArrPartIdLeft = SizeEstimator.estimate(arrPartIdMockLeft)

      val sortLstMockLeft = new SortedLinkedList[Point](k) // at the end of the process, the SortedLinkedList contains k points from the right DS
      var memSortLstLeft = SizeEstimator.estimate(sortLstMockLeft)

      sortLstMockLeft.add(0, Point())
      val memSortLstInsertOverhead = SizeEstimator.estimate(sortLstMockLeft) - SizeEstimator.estimate(Point()) - memSortLstLeft + memMaxPointRight
      memSortLstLeft += k * memSortLstInsertOverhead

      val tupleMockLeft = (0, new RowData())
      val memTupleObjLeft = SizeEstimator.estimate(tupleMockLeft) + memMaxPointLeft + memArrPartIdLeft + memSortLstLeft

      val memLeftRDD = countPointLeft * memTupleObjLeft * (if (isAllKNN) 2 else 1)
      countPartitionsLeft = Math.ceil(memLeftRDD / memMaxPart)

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS memMaxPointLeft: %,d memSortLstLeft: %,d memSortLstInsertOverhead: %,d memTupleObjLeft: %,d memLeftRDD: %,d countPartitionsLeft: %,.2f"
        .format(memMaxPointLeft, memSortLstLeft, memSortLstInsertOverhead, memTupleObjLeft, memLeftRDD, countPartitionsLeft))
    }

    val countPartitionMinRight = Helper.max(countPartitionRight, countPartitionsLeft).toInt
    val countPartitionMaxRight = (math.ceil(countPartitionMinRight / countAllCores.toDouble) * countAllCores - 1).toInt // a multiple of the total number of cores

    val countPartPointMinMaxRight = (countObjRight / countPartitionMaxRight, countObjRight / countPartitionMinRight)

    val rate = maxObjCountRight / countPartPointMinMaxRight._1.toDouble

    if (rate > 1) { // maxGridCellCount has more than coreObjCapacity._1

      gridDim = (gridDim / Math.ceil(Math.sqrt(rate))).toInt

      rddRight_gridXY_Size_MBR_Count.unpersist(false)

      rddRight_gridXY_Size_MBR_Count = rddRight
        .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDim), (SizeEstimator.estimate(point), new MBR(point.x.toInt, point.y.toInt), 1L))))
        .reduceByKey((tuple1, tuple2) => (Helper.max(tuple1._1, tuple2._1), tuple1._2.merge(tuple2._2), tuple1._3 + tuple2._3)) // gets rid of duplicate grid cells but keeps the MBR, max obj size, and # points in each cell
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>memStorageCore: %,d memMaxPart: %,.2f countPartitionMinRight: %,d countPartitionMaxRight: %,d countPartPointMinMaxRight: %s rate: %,.4f gridDim: %,d"
      .format(memStorageCore, memMaxPart, countPartitionMinRight, countPartitionMaxRight, countPartPointMinMaxRight, rate, gridDim))

    val (partitioner_ActiveRight, queueMBR_ActiveRight) = buildPartitioner(rddRight_gridXY_Size_MBR_Count, mbrRight, gridDim, countPartPointMinMaxRight, spatialIndexType)

    if (rddRight_gridXY_Size_MBR_Count.getStorageLevel != StorageLevel.NONE)
      rddRight_gridXY_Size_MBR_Count.unpersist(false)

    (partitioner_ActiveRight, queueMBR_ActiveRight, gridDim)
  }

  private def buildPartitioner(rddRight_gridXY_Size_MBR_Count: RDD[((Int, Int), (Long, MBR, Long))], mbrDS_ActiveRight: MBR, gridDimRightActive: Int, countPartPointMinMaxRight: (Long, Long), spatialIndexType: SupportedSpatialIndexes.Value) = {

    var startTime = System.currentTimeMillis()
    val queueMBR_ActiveRight = mutable.Queue[MBR]()

    var currObjCountMBR = 0L
    var actualNumberOfPartitions = -1

    // build range info
    val iterPartitionerObjects = rddRight_gridXY_Size_MBR_Count
      .mapPartitions(_.map(row => (row._1, row._2._3))) // grid assignment
      .collect()
      .sortBy(_._1) // sorts by (x, y)
      .iterator
      .map(row => { // map cells to partitions

        val newObjCountMBR = currObjCountMBR + row._2

        if (currObjCountMBR == 0 || currObjCountMBR >= countPartPointMinMaxRight._1 || newObjCountMBR > countPartPointMinMaxRight._2) {

          actualNumberOfPartitions += 1
          currObjCountMBR = row._2
          queueMBR_ActiveRight += new MBR(row._1, actualNumberOfPartitions)

          // Helper.loggerSLf4J(debugMode, SparkKnn, ">>new MBR: %d\t%,d\t%,d".format(queueMBR_ActiveRight.length, currObjCountMBR, newObjCountMBR))
        }
        else {

          currObjCountMBR = newObjCountMBR
          queueMBR_ActiveRight.last.update(row._1)
        }

        new Point(row._1._1, row._1._2, new PartitionerPointUserData(row._2, actualNumberOfPartitions))
      })

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>MBRs constructed in %,d MS.".format(System.currentTimeMillis - startTime))

    startTime = System.currentTimeMillis

    // create partitioner
    val partitioner_ActiveRight = fSpatialIndexInit(spatialIndexType)
    partitioner_ActiveRight.insert(buildRectBounds(fComputeGridXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom, gridDimRightActive), fComputeGridXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top, gridDimRightActive)), iterPartitionerObjects, 1)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Partitioner build time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d"
      .format(System.currentTimeMillis - startTime, gridDimRightActive, gridDimRightActive, partitioner_ActiveRight, -1 /*SizeEstimator.estimate(partitioner_ActiveRight)*/))

    (partitioner_ActiveRight, queueMBR_ActiveRight)
  }
}