package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.sknn.util.Helper

import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractIterator, mutable}

object KdTree extends Serializable {

  val nodeCapacity = 4
}

class KdTree extends SpatialIndex {

  var rootNode: KdtNode = _

  def getTotalPoints: Int = rootNode.totalPoints

  override def nodeCapacity: Int = KdTree.nodeCapacity

  override def estimateNodeCount(pointCount: Long): Long = {

    val height = (1.44 * Helper.log(pointCount / nodeCapacity, 2)).toInt

    (1 until height).map(h => Math.pow(2, h + 1).toInt).sum + 1
  }

  override def estimateObjCount(gIdxNodeCount: Int): Long = -1

  @throws(classOf[IllegalStateException])
  def insertIter(rectBounds: Rectangle, iterPoints: Iterator[TraversableOnce[Point]], histogramBarWidth: Int): Unit =
    insert(rectBounds, iterPoints.flatMap(_.seq), histogramBarWidth)

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int): Unit = {

    if (rootNode != null) throw new IllegalStateException("KD Tree already built")
    if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")
    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    if (histogramBarWidth < 1) throw new IllegalStateException("%s%d".format("Histogram bar width must be >= 1: Got: ", histogramBarWidth))

    val queueNodeInfo = mutable.Queue[(KdtBranchRootNode, Boolean, Histogram, Histogram)]()

    val lowerBounds = (rectBounds.left, rectBounds.bottom)

    def buildNode(nodeAVLSplitInfo: Histogram, splitX: Boolean): KdtNode = {

      if (nodeAVLSplitInfo.pointCount <= nodeCapacity)
        new KdtLeafNode(nodeAVLSplitInfo.extractPointInfo)
      else {

        val avlSplitInfoParts = nodeAVLSplitInfo.partition(splitX)

        val kdtBranchRootNode = new KdtBranchRootNode(avlSplitInfoParts._1, nodeAVLSplitInfo.pointCount)

        queueNodeInfo += ((kdtBranchRootNode, splitX, avlSplitInfoParts._2, avlSplitInfoParts._3))

        kdtBranchRootNode
      }
    }

    rootNode = buildNode(Histogram(iterPoints, histogramBarWidth, lowerBounds), splitX = true)

    while (queueNodeInfo.nonEmpty) {

      val (currNode, splitX, avlSplitInfoLeft, avlSplitInfoRight) = queueNodeInfo.dequeue

      if (avlSplitInfoLeft != null)
        currNode.left = buildNode(avlSplitInfoLeft, !splitX)
      if (avlSplitInfoRight != null)
        currNode.right = buildNode(avlSplitInfoRight, !splitX)
    }

    updateBoundsAndTotalPoint()
  }

  override def findExact(searchXY: (Double, Double)): Point = {
    //if(searchXY._1.toInt==248 && searchXY._2.toInt==58)
    //  println
    val stackNode = mutable.Stack((this.rootNode, true))

    while (stackNode.nonEmpty) {

      val (currNode, splitX) = stackNode.pop

      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if (kdtBRN.splitVal == SPLIT_VAL_NONE)
            stackNode.push((kdtBRN.left, !splitX), (kdtBRN.right, !splitX))
          else if ((if (splitX) searchXY._1 else searchXY._2) <= kdtBRN.splitVal)
            stackNode.push((kdtBRN.left, !splitX))
          else
            stackNode.push((kdtBRN.right, !splitX))

        case kdtLeafNode: KdtLeafNode =>

          val optPoint = kdtLeafNode.arrPoints.find(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2))

          if (optPoint.nonEmpty)
            return optPoint.get
      }
    }

    null
  }

  def printIndented(): Unit =
    printIndented(rootNode, "", isLeft = false)

  private def printIndented(node: KdtNode, indent: String, isLeft: Boolean): Unit = {

    if (node != null) {

      println(indent + (if (isLeft) "|__" else "|__") + node)

      node match {
        case kdtBRN: KdtBranchRootNode =>
          printIndented(kdtBRN.left, indent + (if (isLeft) "|  " else "   "), isLeft = true)
          printIndented(kdtBRN.right, indent + (if (isLeft) "|  " else "   "), isLeft = false)
        case _ =>
      }
    }
  }

  def findBestNode(searchPoint: Geom2D, minCount: Int): KdtNode = {

    // find leaf containing point
    val fNodeCheck = (kdtNode: KdtNode) => kdtNode != null && kdtNode.totalPoints >= minCount && kdtNode.rectNodeBounds.contains(searchPoint)
    var currNode = rootNode
    var splitX = true

    while (currNode != null)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if (kdtBRN.splitVal == SPLIT_VAL_NONE)
            if (fNodeCheck(kdtBRN.left))
              currNode = kdtBRN.left
            else if (fNodeCheck(kdtBRN.right))
              currNode = kdtBRN.right
            else
              return kdtBRN
          else if ((if (splitX) searchPoint.x else searchPoint.y) <= kdtBRN.splitVal)
            if (fNodeCheck(kdtBRN.left))
              currNode = kdtBRN.left
            else
              return currNode
          else if (fNodeCheck(kdtBRN.right))
            currNode = kdtBRN.right
          else
            return currNode

          splitX = !splitX

        case _: KdtNode =>
          return currNode //(currNode, splitX)
      }

    null
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]): Unit = {

    val knnLookupInfo = KnnLookupInfo(searchPoint, sortSetSqDist)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode): Unit = {

      val queueKdtNode = mutable.Queue(kdtNode)

      while (queueKdtNode.nonEmpty) {

        val kdt = queueKdtNode.dequeue()

        if (kdt != skipKdtNode)
          kdt match {
            case kdtBRN: KdtBranchRootNode =>

              if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                queueKdtNode += kdtBRN.left
              if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                queueKdtNode += kdtBRN.right

            case kdtLeafNode: KdtLeafNode =>
              if (knnLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.arrPoints
                  .foreach(point =>
                    if (knnLookupInfo.rectSearchRegion.contains(point.x, point.y))
                      testAndAddPoint(point, knnLookupInfo))
          }
      }
    }

    if (sortSetSqDist.isFull)
      process(this.rootNode, null)
    else {

      val sPtBestNode = if (sortSetSqDist.isFull) rootNode else findBestNode(searchPoint, sortSetSqDist.maxSize)

      process(sPtBestNode, null)
      process(this.rootNode, sPtBestNode)
    }
  }

  override def toString: String =
    rootNode.toString

  override def write(kryo: Kryo, output: Output): Unit = {

    val qNode = mutable.Queue(this.rootNode)

    while (qNode.nonEmpty) {

      val kdtNode = qNode.dequeue()

      kryo.writeClassAndObject(output, kdtNode)

      kdtNode match {
        case brn: KdtBranchRootNode =>
          qNode += (brn.left, brn.right)
        case _ =>
      }
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    def readNode() = kryo.readClassAndObject(input) match {
      case kdtNode: KdtNode => kdtNode
      case _ => null
    }

    this.rootNode = readNode()

    val qNode = mutable.Queue(this.rootNode)

    while (qNode.nonEmpty)
      qNode.dequeue() match {
        case kdtBranchRootNode: KdtBranchRootNode =>
          kdtBranchRootNode.left = readNode()
          kdtBranchRootNode.right = readNode()

          qNode += (kdtBranchRootNode.left, kdtBranchRootNode.right)
        case _ =>
      }
  }

  private def updateBoundsAndTotalPoint(): Unit =
    rootNode match {
      case kdtBRN: KdtBranchRootNode =>

        val stackNode = mutable.Stack[KdtBranchRootNode](kdtBRN)
        //        val stackRoots = mutable.Stack[KdtBranchRootNode]()

        while (stackNode.nonEmpty) {

          //          stackRoots.push(stackNode.pop)
          val currNode = stackNode.top

          currNode.left match {
            case kdtBRN: KdtBranchRootNode =>
              if (currNode.left.rectNodeBounds == null)
                stackNode.push(kdtBRN)
            case _ =>
          }

          currNode.right match {
            case kdtBRN: KdtBranchRootNode =>
              if (currNode.right.rectNodeBounds == null)
                stackNode.push(kdtBRN)
            case _ =>
          }

          if (currNode == stackNode.top) {

            stackNode.pop

            if (currNode.left != null)
              currNode.rectNodeBounds = new Rectangle(currNode.left.rectNodeBounds)

            if (currNode.right != null)
              if (currNode.rectNodeBounds == null)
                currNode.rectNodeBounds = new Rectangle(currNode.right.rectNodeBounds)
              else
                currNode.rectNodeBounds.mergeWith(currNode.right.rectNodeBounds)
          }
        }
      case _ =>
    }

  override def allPoints: Iterator[ArrayBuffer[Point]] = new AbstractIterator[ArrayBuffer[Point]] {

    private val queueNode = mutable.Queue[KdtNode](rootNode)

    override def hasNext: Boolean = queueNode.nonEmpty

    override def next(): ArrayBuffer[Point] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {

        var ans: ArrayBuffer[Point] = null

        while (ans == null && queueNode.nonEmpty)
          ans = this.queueNode.dequeue match {
            case kdtBRN: KdtBranchRootNode =>
              this.queueNode += (kdtBRN.left, kdtBRN.right)
              null
            case kdtLeafNode: KdtLeafNode =>
              kdtLeafNode.arrPoints
            case _ =>
              null
          }

        ans
      }
  }
}