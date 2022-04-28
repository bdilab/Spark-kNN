package org.cusp.bdi.ds.geom

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

case class Point() extends Geom2D {

  var userData: AnyRef = _

  def this(x: Double, y: Double) = {

    this()

    this.x = x
    this.y = y
  }

  def this(xy: Double) =
    this(xy, xy)

  def this(other: Point) = {

    this(other.x, other.y)
    this.userData = other.userData
  }

  def this(x: Double, y: Double, userData: AnyRef) = {

    this(x, y)

    this.userData = userData
  }

  def this(xy: (Double, Double), userData: AnyRef) =
    this(xy._1, xy._2, userData)

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  //  override def equals(other: Any): Boolean = other match {
  //    case pt: Point =>
  //      if (userData == null || pt.userData == null)
  //        this.x.equals(pt.x) && this.y.equals(pt.y)
  //      else
  //        userData.equals(pt.userData)
  //    case _ => false
  //  }

  override def toString: String =
    "(%s,%s)".format(super.toString, if (userData == null) "" else userData.toString)

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)

    kryo.writeClassAndObject(output, userData)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)

    userData = kryo.readClassAndObject(input)
  }
}
