package org.cusp.bdi.ds.sortset

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.geom.Point

import scala.collection.AbstractIterator
import scala.collection.immutable.Iterable

class Node[T <: Serializable]() extends KryoSerializable {

  var distance: Double = Double.NegativeInfinity
  var data: T = _
  var next: Node[T] = _

  def this(distance: Double, data: T) {

    this()
    this.distance = distance
    this.data = data
  }

  override def toString: String =
    "%,.8f, %s".format(distance, data)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeDouble(distance)
    kryo.writeClassAndObject(output, data)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    distance = input.readDouble()
    data = kryo.readClassAndObject(input).asInstanceOf[T]
  }
}

case class SortedLinkedList[T <: Serializable]() extends KryoSerializable with Iterable[Node[T]] {

  var maxSize: Int = Int.MaxValue
  private var headNode: Node[T] = _
  private var lastNode: Node[T] = _
  private var nodeCount = 0

  require(maxSize > 0, "Invalid maxSize parameter. Values > 0 only")

  def this(maxSize: Int) = {

    this()
    this.maxSize = maxSize
  }

  def clear(): Unit = {

    headNode = null
    lastNode = null
    nodeCount = 0
  }

  def addAll(other: SortedLinkedList[T]): SortedLinkedList[T] = {

    other.foreach(node => add(node.distance, node.data))

    this
  }

  def add(distance: Double, data: T): Unit =
    if (nodeCount < maxSize || distance < last.distance) {

      var prevNode: Node[T] = null
      var currNode = headNode
      var prevNodeIndex = -1

      if (last.ne(null) && distance >= last.distance) {

        prevNodeIndex += nodeCount
        prevNode = last
        currNode = null
      }
      else if (currNode ne null)
        while (distance > currNode.distance) {

          prevNodeIndex += 1

          prevNode = currNode
          currNode = currNode.next
        }

      nodeCount += 1

      if (prevNode eq null) { // insert first

        headNode = new Node(distance, data)
        headNode.next = currNode

        if (lastNode eq null)
          lastNode = headNode
      }
      else { // insert after

        prevNode.next = new Node(distance, data)
        prevNode.next.next = currNode

        if (lastNode eq prevNode)
          lastNode = prevNode.next
      }

      if (nodeCount > maxSize) {

        if (prevNode eq null) {

          prevNode = headNode
          prevNodeIndex = 0
        }

        // jump to maxSize node
        while (prevNodeIndex < maxSize - 1) {

          prevNode = prevNode.next
          prevNodeIndex += 1
        }

        if (prevNode ne lastNode) {

          lastNode = prevNode
          lastNode.next = null
          nodeCount = prevNodeIndex + 1
        }
      }
    }

  def isFull: Boolean = nodeCount >= maxSize

  def stopAt(node: Node[T], nodeCount: Int): Unit =
    if (node.ne(null) && node.ne(lastNode)) {

      lastNode = node
      lastNode.next = null
      this.nodeCount = nodeCount
    }

  def get(idx: Int): Node[T] = {

    var current = head
    var i = 0
    while (i < idx) {

      current = current.next
      i += 1
    }

    current
  }

  override def head: Node[T] = headNode

  override def last: Node[T] = lastNode

  def length: Int = nodeCount

  def capacity: Int = maxSize

  override def size: Int = nodeCount

  override def iterator: Iterator[Node[T]] = new AbstractIterator[Node[T]] {

    var cursor: Node[T] = if (SortedLinkedList.this.isEmpty()) null else headNode

    override def hasNext: Boolean = cursor ne null

    override def next(): Node[T] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {
        val ans = cursor
        cursor = cursor.next
        ans
      }
  }

  override def isEmpty(): Boolean = headNode eq null

  override def toString(): String =
    mkString("\n")

  override def mkString(sep: String): String =
    this.map(node => "%s%s".format(node, sep)).mkString("")

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(maxSize)
    output.writeInt(nodeCount)

    var currNode = head

    while (currNode ne null) {

      kryo.writeClassAndObject(output, currNode)
      currNode = currNode.next
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    maxSize = input.readInt()
    nodeCount = input.readInt()

    if (nodeCount > 0) {

      headNode = kryo.readClassAndObject(input).asInstanceOf[Node[T]]
      lastNode = headNode

      for (_ <- 1 until nodeCount) {

        lastNode.next = kryo.readClassAndObject(input).asInstanceOf[Node[T]]
        lastNode = lastNode.next
      }
    }
  }
}
