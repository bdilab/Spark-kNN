package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.collection.mutable.ArrayBuffer

/*
 [@specialized(Float, Double) T: Fractional]
  val fractionalOps = implicitly[Fractional[T]]

  import fractionalOps._
 */

final class MBR extends KryoSerializable with Serializable {

  var left: Int = Int.MaxValue
  var bottom: Int = Int.MaxValue
  var right: Int = Int.MinValue
  var top: Int = Int.MinValue
  var assignedPartition: Int = -1

  def this(seedX: Int, seedY: Int) = {

    this()

    this.left = seedX
    this.bottom = seedY
    this.right = left
    this.top = bottom
  }

  def this(seedXY: (Int, Int)) =
    this(seedXY._1, seedXY._2)

  def this(seedXY: (Int, Int), assignedPartition: Int) = {

    this(seedXY)
    this.assignedPartition = assignedPartition
  }

  def update(newGridXY: (Int, Int)): Unit = {

    right = newGridXY._1

    if (newGridXY._2 < bottom)
      bottom = newGridXY._2
    else if (newGridXY._2 > top)
      top = newGridXY._2
  }

  def merge(other: MBR): MBR = {

    if (other.left < left) left = other.left
    if (other.bottom < bottom) bottom = other.bottom
    if (other.right > right) right = other.right
    if (other.top > top) top = other.top

    this
  }

  override def toString: String =
    "%,d\t%,d\t%,d\t%,d\t%,d".format(left, bottom, right, top, assignedPartition)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(left)
    output.writeInt(bottom)
    output.writeInt(right)
    output.writeInt(top)
    output.writeInt(assignedPartition)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    left = input.readInt()
    bottom = input.readInt()
    right = input.readInt()
    top = input.readInt()
    assignedPartition = input.readInt()
  }
}

final class RowData extends KryoSerializable {

  var point: Point = _
  var sortedList: SortedLinkedList[Point] = _
  var arrPartitionId: ArrayBuffer[Int] = _

  def this(point: Point, sortedList: SortedLinkedList[Point], arrPartitionId: ArrayBuffer[Int]) = {

    this()

    this.point = point
    this.sortedList = sortedList
    this.arrPartitionId = arrPartitionId
  }

  def nextPartId(): Int =
    if (arrPartitionId.isEmpty)
      -1
    else {

      val pId = arrPartitionId.head

      arrPartitionId = arrPartitionId.tail

      pId
    }

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeObject(output, point)
    kryo.writeObject(output, sortedList)

    output.writeInt(arrPartitionId.length)
    arrPartitionId.foreach(output.writeInt)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    point = kryo.readObject(input, classOf[Point])
    sortedList = kryo.readObject(input, classOf[SortedLinkedList[Point]])

    val arrLength = input.readInt()

    arrPartitionId = new ArrayBuffer[Int]()
    arrPartitionId.sizeHint(arrLength)

    for (_ <- 0 until arrLength)
      arrPartitionId += input.readInt
  }
}