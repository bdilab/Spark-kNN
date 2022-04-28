package org.cusp.bdi.ds.geom

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.sknn.util.Helper

class Rectangle() extends KryoSerializable with Serializable {

  var center: Geom2D = _
  var halfXY: Geom2D = _

  def this(center: Geom2D, halfXY: Geom2D) {

    this()
    this.center = center
    this.halfXY = halfXY
  }

  def this(other: Rectangle) =
    this(new Geom2D(other.center), new Geom2D(other.halfXY))

  def this(geom2D: Geom2D) =
    this(geom2D, geom2D)

  def contains(x: Double, y: Double): Boolean =
    !(x < left || x > right || y < bottom || y > top)

  def contains(point: Geom2D): Boolean =
    contains(point.x, point.y)

  def contains(xy: (Double, Double)): Boolean =
    contains(xy._1, xy._2)

  def mergeWith(other: Rectangle): Unit =
    if (other != null) {

      val minX = Helper.min(this.left, other.left)
      val minY = Helper.min(this.bottom, other.bottom)
      val maxX = Helper.max(this.right, other.right)
      val maxY = Helper.max(this.top, other.top)

      this.halfXY.x = (maxX - minX) / 2
      this.halfXY.y = (maxY - minY) / 2

      this.center.x = minX + this.halfXY.x
      this.center.y = minY + this.halfXY.y
    }

  def left: Double =
    center.x - halfXY.x

  def bottom: Double =
    center.y - halfXY.y

  def right: Double =
    center.x + halfXY.x

  def top: Double =
    center.y + halfXY.y

  def intersects(other: Rectangle): Boolean = {

    lazy val otherLeft = other.left
    lazy val otherBottom = other.bottom
    lazy val otherRight = other.right
    lazy val otherTop = other.top

    intersects(otherLeft, otherBottom, otherRight, otherTop)
  }

  def intersects(otherLeft: => Double, otherBottom: => Double, otherRight: => Double, otherTop: => Double): Boolean = {

    lazy val thisLeft = this.left
    lazy val thisBottom = this.bottom
    lazy val thisRight = this.right
    lazy val thisTop = this.top

    !(thisLeft > otherRight ||
      thisRight < otherLeft ||
      thisTop < otherBottom ||
      thisBottom > otherTop)
  }

  override def toString: String =
    "%s\t%s".format(center, halfXY)

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeClassAndObject(output, center)
    kryo.writeClassAndObject(output, halfXY)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    center = kryo.readClassAndObject(input) match {
      case geom2D: Geom2D => geom2D
    }

    halfXY = kryo.readClassAndObject(input) match {
      case geom2D: Geom2D => geom2D
    }
  }
}