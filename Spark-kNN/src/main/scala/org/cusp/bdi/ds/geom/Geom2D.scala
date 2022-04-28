package org.cusp.bdi.ds.geom

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

class Geom2D() extends KryoSerializable with Serializable {

  var x = 0.0
  var y = 0.0

  def this(x: Double, y: Double) = {

    this()

    this.x = x
    this.y = y
  }

  def this(dim: Double) =
    this(dim, dim)

  def this(geom2D: Geom2D) =
    this(geom2D.x, geom2D.y)

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeDouble(x)
    output.writeDouble(y)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    x = input.readDouble()
    y = input.readDouble()
  }

  override def toString: String =
    "(%.10f,%.10f)".format(x, y)
}
