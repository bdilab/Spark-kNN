package org.cusp.bdi.ds.kdt

import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.bt.{AVLNode, AVLTree}
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.Histogram.{TypeAVL, TypeAVL_Data, fAddToAVL}
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Histogram {

  type TypeAVL_Data = ListBuffer[(Int, Point)]
  type TypeAVL = AVLTree[TypeAVL_Data]

  val fAddToAVL: ((Int, Point), TypeAVL, Int) => TypeAVL_Data = (idxPoint: (Int, Point), avlTree: TypeAVL, currNodeVal: Int) => {

    val avlNode = avlTree.findOrElseInsert(idxPoint._1)

    if (avlNode.data == null)
      avlNode.data = new TypeAVL_Data()

    avlNode.data += ((currNodeVal, idxPoint._2))
  }

  def apply(iterPoints: Iterator[Point], hgBarWidth: Int, lowerBounds: (Double, Double)): Histogram = {

    val setCoordY = mutable.Set[Int]()
    val avlHistogram = new TypeAVL()
    val fComputeCoord =
      if (hgBarWidth == 1) (point: Point) => ((point.x - lowerBounds._1).toInt, (point.y - lowerBounds._2).toInt)
      else (point: Point) => (((point.x - lowerBounds._1) / hgBarWidth).toInt, ((point.y - lowerBounds._2) / hgBarWidth).toInt)

    var pointCount = 0

    iterPoints
      .foreach(pt => {

        pointCount += 1

        val (idxX, idxY) = fComputeCoord(pt)

        setCoordY += idxY

        fAddToAVL((idxX, pt), avlHistogram, idxY)
        //        val avlNode = avlHistogram.findOrElseInsert(idxX.toInt)
        //
        //        if (avlNode.data == null)
        //          avlNode.data = new TypeAVL_Data()
        //
        //        avlNode.data += ((idxY.toInt, pt))
      })

    new Histogram(avlHistogram, setCoordY.size, pointCount)
  }
}

case class Histogram(avlTree: TypeAVL, otherIndexCount: Int, pointCount: Int) {

  def extractPointInfo: (ArrayBuffer[Point], Rectangle) = {

    val arrPoints = ArrayBuffer[Point]()

    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    avlTree.allNodes
      .foreach(_.data.foreach(row => {

        arrPoints += row._2

        if (row._2.x < minX) minX = row._2.x
        if (row._2.x > maxX) maxX = row._2.x

        if (row._2.y < minY) minY = row._2.y
        if (row._2.y > maxY) maxY = row._2.y
      }))

    (arrPoints, buildRectBounds(minX, minY, maxX, maxY))
  }

  def partition(splitX: Boolean): (Double, Histogram, Histogram) = {

    val fExtractCoord = (point: Point) => if (splitX) point.x else point.y
    val pointCountHalf = pointCount / 2
    var pointCountPart1 = 0
    val stackNode = mutable.Stack[AVLNode[TypeAVL_Data]]()
    var currNode = avlTree.rootNode
    var lastNodePart1: Point = null
    val avlHistogramPart1 = new TypeAVL()
    val avlHistogramPart2 = new TypeAVL()
    var indexCountPart1 = 0 // counts the number of indexes subsumed into the new AVL tree. represents the number of indexes redirected to the new AVL
    var indexCountPart2 = 0

    // inorder traversal
    while (currNode != null || stackNode.nonEmpty) {

      // left-most node
      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop

      if (pointCountPart1 >= pointCountHalf) {

        indexCountPart2 += 1

        currNode.data.foreach(fAddToAVL(_, avlHistogramPart2, currNode.nodeVal))
      }
      else {

        indexCountPart1 += 1

        val iterData = (if (pointCountPart1 + currNode.data.length >= pointCountHalf) currNode.data.sortBy(idxPoint => fExtractCoord(idxPoint._2)) else currNode.data).iterator

        while (iterData.hasNext) {

          val idxPoint = iterData.next

          if (pointCountPart1 < pointCountHalf) {

            pointCountPart1 += 1

            lastNodePart1 = idxPoint._2

            fAddToAVL(idxPoint, avlHistogramPart1, currNode.nodeVal)
          }
          else {

            indexCountPart2 += 1

            if (fExtractCoord(lastNodePart1) == fExtractCoord(idxPoint._2))
              lastNodePart1 = null

            fAddToAVL(idxPoint, avlHistogramPart2, currNode.nodeVal)

            iterData.foreach(fAddToAVL(_, avlHistogramPart2, currNode.nodeVal))
          }
        }
      }

      currNode = currNode.right
    }

    val splitInfoPart1 = new Histogram(avlHistogramPart1, indexCountPart1, pointCountPart1)

    val splitInfoPart2 =
      if (pointCount - pointCountPart1 == 0)
        null
      else
        new Histogram(avlHistogramPart2, indexCountPart2, pointCount - pointCountPart1)

    (if (lastNodePart1 == null) SPLIT_VAL_NONE else fExtractCoord(lastNodePart1), splitInfoPart1, splitInfoPart2)
  }
}