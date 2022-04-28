package org.cusp.bdi.ds.bt

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, mutable}

class AVLTree[T] extends Serializable {

  var rootNode: AVLNode[T] = _

  def lookupValue(valueLookup: Int): AVLNode[T] = {

    var currNode = rootNode

    while (currNode != null)
      valueLookup.compare(currNode.nodeVal).signum match {
        case -1 =>
          currNode = currNode.left
        case 0 =>
          return currNode
        case 1 =>
          currNode = currNode.right
      }

    null
  }

  def findOrElseInsert(newValue: Int): AVLNode[T] = {

    def updateParentLink(stackNodes: mutable.Stack[AVLNode[T]], currNode: AVLNode[T], newSubtreeRoot: AVLNode[T]): Unit =
      if (stackNodes.nonEmpty)
        currNode.nodeVal.compare(stackNodes.top.nodeVal).signum match {
          case -1 =>
            stackNodes.top.left = newSubtreeRoot
          case _ =>
            stackNodes.top.right = newSubtreeRoot
        }

    var newNode = new AVLNode[T](newValue)

    if (rootNode == null)
      rootNode = newNode
    else {

      var stackNodes = mutable.Stack[AVLNode[T]]()

      var currNode = rootNode

      while (currNode != null) {

        stackNodes.push(currNode)

        newValue.compare(currNode.nodeVal).signum match {
          case -1 =>
            if (currNode.left == null) {

              currNode.left = newNode
              currNode.treeHeight = 1 + max(nodeHeight(currNode.left), nodeHeight(currNode.right))
              stackNodes.pop
              currNode = null
            }
            else
              currNode = currNode.left
          case 0 =>
            newNode = currNode
            stackNodes = mutable.Stack[AVLNode[T]]()
            currNode = null
          case 1 =>
            if (currNode.right == null) {

              currNode.right = newNode
              currNode.treeHeight = 1 + max(nodeHeight(currNode.left), nodeHeight(currNode.right))
              stackNodes.pop
              currNode = null
            }
            else
              currNode = currNode.right
        }
      }

      while (stackNodes.nonEmpty) {

        currNode = stackNodes.pop

        currNode.treeHeight = 1 + max(nodeHeight(currNode.left), nodeHeight(currNode.right))

        val hDiff = nodeHeight(currNode.left) - nodeHeight(currNode.right)

        if (hDiff.abs > 1)
          hDiff.signum match {
            case -1 =>
              if (newValue < currNode.right.nodeVal)
                currNode.right = rotateRight(currNode.right)

              updateParentLink(stackNodes, currNode, rotateLeft(currNode))
            case _ =>
              if (newValue > currNode.left.nodeVal)
                currNode.left = rotateLeft(currNode.left)
              updateParentLink(stackNodes, currNode, rotateRight(currNode))
          }
      }
    }

    newNode
  }

  //  private def getOrElseInsert(currNode: AVLNode[T], newNode: AVLNode[T]): (AVLNode[T], AVLNode[T]) = {
  //
  //    var valueNode = rootNode
  //
  //    if (currNode == null) {
  //
  //      valueNode = newNode
  //
  //      return (valueNode, valueNode)
  //    } else {
  //
  //      newNode.nodeValue.compare(currNode.nodeValue).signum match {
  //        case -1 =>
  //
  //          val res = getOrElseInsert(currNode.left, newNode)
  //          currNode.left = res._1
  //          valueNode = res._2
  //        case 0 =>
  //          return (currNode, currNode)
  //        case 1 =>
  //
  //          val res = getOrElseInsert(currNode.right, newNode)
  //          currNode.right = res._1
  //          valueNode = res._2
  //      }
  //
  //      currNode.treeHeight = 1 + AVLTree.max(nodeHeight(currNode.left), nodeHeight(currNode.right))
  //
  //      val hDiff = heightDiff(currNode)
  //
  //      if (hDiff > 1) {
  //
  //        val keyDiff = newNode.nodeValue - currNode.left.nodeValue
  //
  //        if (keyDiff < 0)
  //          return (rotateRight(currNode), valueNode)
  //        else if (keyDiff > 0) {
  //
  //          currNode.left = rotateLeft(currNode.left)
  //          return (rotateRight(currNode), valueNode)
  //        }
  //      }
  //      else if (hDiff < -1) {
  //
  //        val keyDiff = newNode.nodeValue - currNode.right.nodeValue
  //
  //        if (keyDiff < 0) {
  //
  //          currNode.right = rotateRight(currNode.right)
  //          return (rotateLeft(currNode), valueNode)
  //        }
  //        else if (keyDiff > 0)
  //          return (rotateLeft(currNode), valueNode)
  //      }
  //    }
  //
  //    (currNode, valueNode)
  //  }

  def breadthFirst: Iterator[AVLNode[T]] = new AbstractIterator[AVLNode[T]] {

    private val queue = if (rootNode == null) null else mutable.Queue(rootNode)

    override def hasNext: Boolean = queue != null && queue.nonEmpty

    override def next(): AVLNode[T] =
      if (hasNext) {

        val ret = queue.dequeue()

        if (ret.left != null)
          queue += ret.left

        if (ret.right != null)
          queue += ret.right

        ret
      }
      else
        throw new NoSuchElementException("next on empty Iterator")
  }

  def allNodes: Iterator[AVLNode[T]] = new AbstractIterator[AVLNode[T]] {

    private val stack = if (rootNode == null) null else mutable.Stack(rootNode)

    override def hasNext: Boolean = stack != null && stack.nonEmpty

    override def next(): AVLNode[T] =
      if (hasNext) {

        val ret = stack.pop()

        if (ret.left != null)
          stack.push(ret.left)

        if (ret.right != null)
          stack.push(ret.right)

        ret
      }
      else
        throw new NoSuchElementException("next on empty Iterator")
  }

  def printIndented(): Unit =
    printIndented(rootNode, "", isLeft = false)

  private def printIndented(node: AVLNode[T], indent: String, isLeft: Boolean): Unit = {

    if (node != null) {

      println(indent + (if (isLeft) "|__" else "|__") + node.nodeVal)
      printIndented(node.left, indent + (if (isLeft) "|  " else "   "), isLeft = true)
      printIndented(node.right, indent + (if (isLeft) "|  " else "   "), isLeft = false)
    }
  }

  def inOrder(): ListBuffer[AVLNode[T]] = {

    val retLst = ListBuffer[AVLNode[T]]()

    val stackNode = mutable.Stack[AVLNode[T]]()
    var currNode = rootNode

    while (currNode != null || stackNode.nonEmpty) {

      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop
      retLst += currNode

      currNode = currNode.right
    }

    retLst
  }

  override def toString: String =
    rootNode.toString

  private def rotateRight(nodePivot: AVLNode[T]) = {

    val newSubtreeRoot = nodePivot.left
    val temp = newSubtreeRoot.right

    // rotate
    newSubtreeRoot.right = nodePivot
    nodePivot.left = temp

    nodePivot.treeHeight = max(nodeHeight(nodePivot.left), nodeHeight(nodePivot.right)) + 1
    newSubtreeRoot.treeHeight = max(nodeHeight(newSubtreeRoot.left), nodeHeight(newSubtreeRoot.right)) + 1

    if (nodePivot == rootNode)
      rootNode = newSubtreeRoot

    newSubtreeRoot
  }

  private def rotateLeft(nodePivot: AVLNode[T]) = {

    val newSubtreeRoot = nodePivot.right
    val temp = newSubtreeRoot.left

    // rotate
    newSubtreeRoot.left = nodePivot
    nodePivot.right = temp

    nodePivot.treeHeight = max(nodeHeight(nodePivot.left), nodeHeight(nodePivot.right)) + 1
    newSubtreeRoot.treeHeight = max(nodeHeight(newSubtreeRoot.left), nodeHeight(newSubtreeRoot.right)) + 1

    if (nodePivot == rootNode)
      rootNode = newSubtreeRoot

    newSubtreeRoot
  }

  private def nodeHeight(node: AVLNode[_]): Int =
    node match {
      case null => 0
      case _ => node.treeHeight
    }

  private def max(x: Int, y: Int): Int =
    if (x > y) x else y
}