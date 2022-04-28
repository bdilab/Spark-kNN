package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE

import scala.collection.mutable.ArrayBuffer

object KdtNode {
  val SPLIT_VAL_NONE: Double = Double.NegativeInfinity
}

abstract class KdtNode extends KryoSerializable {

  def totalPoints: Int

  var rectNodeBounds: Rectangle = _

  override def toString: String =
    "%s\t%,d".format(rectNodeBounds, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit =
    kryo.writeObject(output, rectNodeBounds)

  override def read(kryo: Kryo, input: Input): Unit =
    rectNodeBounds = kryo.readObject(input, classOf[Rectangle])
}

final class KdtBranchRootNode extends KdtNode {

  private var _totalPoints: Int = 0
  var splitVal: Double = SPLIT_VAL_NONE
  var left: KdtNode = _
  var right: KdtNode = _

  override def totalPoints: Int = _totalPoints

  def this(splitVal: Double, totalPoints: Int) = {

    this()
    this.splitVal = splitVal
    this._totalPoints = totalPoints
  }

  override def toString: String =
    "%s\t[%,.4f]\t%s %s".format(super.toString, splitVal, if (left == null) '-' else '/', if (right == null) '-' else '\\')

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)
    output.writeInt(_totalPoints)
    output.writeDouble(splitVal)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)
    _totalPoints = input.readInt()
    splitVal = input.readDouble()
  }
}

final class KdtLeafNode extends KdtNode {

  var arrPoints: ArrayBuffer[Point] = _

  override def totalPoints: Int = arrPoints.length

  def this(nodeInfo: (ArrayBuffer[Point], Rectangle)) = {

    this()
    this.arrPoints = nodeInfo._1
    this.rectNodeBounds = nodeInfo._2
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)

    output.writeInt(arrPoints.length)
    arrPoints.foreach(kryo.writeObject(output, _))
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)

    val arrLength = input.readInt()

    arrPoints = ArrayBuffer[Point]()
    arrPoints.sizeHint(arrLength)

    (0 until arrLength).foreach(_ => arrPoints += kryo.readObject(input, classOf[Point]))
  }
}