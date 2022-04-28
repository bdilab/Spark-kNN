package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.sknn.util.Helper

import scala.collection.mutable.ArrayBuffer

object SpatialIndex extends Serializable {

  case class KnnLookupInfo(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]) {

    var limitSquaredDist: Double = if (sortSetSqDist.isFull) sortSetSqDist.last.distance else Double.MaxValue
    val rectSearchRegion: Rectangle = new Rectangle(this.searchPoint, new Geom2D(math.sqrt(this.limitSquaredDist)))
  }

  def buildRectBounds(mbrEnds: ((Int, Int), (Int, Int))): Rectangle =
    buildRectBounds(mbrEnds._1._1, mbrEnds._1._2, mbrEnds._2._1, mbrEnds._2._2)

  def buildRectBounds(minX: Double, minY: Double, maxX: Double, maxY: Double): Rectangle = {

    val halfXY = new Geom2D((maxX - minX) / 2, (maxY - minY) / 2)

    new Rectangle(new Geom2D(minX + halfXY.x, minY + halfXY.y), halfXY)
  }

  def testAndAddPoint(point: Point, knnLookupInfo: KnnLookupInfo): Unit = {

    val sqDist = Helper.euclideanDistSquared(knnLookupInfo.rectSearchRegion.center.x, knnLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    knnLookupInfo.sortSetSqDist.add(sqDist, point)

    if (knnLookupInfo.sortSetSqDist.isFull && knnLookupInfo.limitSquaredDist != knnLookupInfo.sortSetSqDist.last.distance) {

      knnLookupInfo.limitSquaredDist = knnLookupInfo.sortSetSqDist.last.distance //+ FLOAT_ERROR_RANGE // double precision errors

      knnLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(knnLookupInfo.limitSquaredDist)
      knnLookupInfo.rectSearchRegion.halfXY.y = knnLookupInfo.rectSearchRegion.halfXY.x
    }
  }
}

trait SpatialIndex extends KryoSerializable {

  def nodeCapacity: Int

  def estimateNodeCount(pointCount: Long): Long

  def estimateObjCount(gIdxNodeCount: Int): Long

  def getTotalPoints: Int

  @throws(classOf[IllegalStateException])
  def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int): Unit

  @throws(classOf[IllegalStateException])
  def insertIter(rectBounds: Rectangle, iterPoints: Iterator[TraversableOnce[Point]], histogramBarWidth: Int): Unit

  def findExact(searchXY: (Double, Double)): Point

  def allPoints: Iterator[ArrayBuffer[Point]]

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]): Unit
}
