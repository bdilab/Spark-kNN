package org.cusp.bdi.ds.geom

case class Circle(center: Geom2D) extends Serializable {

  private var radius = 0.0

  def setRadius(radius: Double): Unit =
    this.radius = radius

  def getRadius: Double = radius

  def this(center: Geom2D, radius: Double) = {

    this(center)
    this.radius = radius
  }

  def left: Double = this.center.x - this.radius

  def right: Double = this.center.x + this.radius

  def top: Double = this.center.y + this.radius

  def bottom: Double = this.center.y - this.radius

  def contains(rect: Rectangle): Boolean = {

    lazy val rectLeft = rect.left
    lazy val rectRight = rect.right
    lazy val rectTop = rect.top
    lazy val rectBottom = rect.bottom

    contains(rectLeft, rectBottom, rectRight, rectTop)
  }

  def intersects(mbr: (Double, Double, Double, Double)): Boolean =
    intersects(mbr._1, mbr._2, mbr._3, mbr._4)

  def contains(mbr: (Double, Double, Double, Double)): Boolean =
    contains(mbr._1, mbr._2, mbr._3, mbr._4)

  def contains(left: => Double, bottom: => Double, right: => Double, top: => Double): Boolean = {

    lazy val circleLeft = this.left
    lazy val circleRight = this.right
    lazy val circleTop = this.top
    lazy val circleBottom = this.bottom

    !(left <= circleLeft || right >= circleRight ||
      top >= circleTop || bottom <= circleBottom)
  }

  def intersects(otherLeft: => Double, otherBottom: => Double, otherRight: => Double, otherTop: => Double): Boolean = {

    lazy val circleLeft = this.center.x - this.radius
    lazy val circleRight = this.center.x + this.radius
    lazy val circleTop = this.center.y + this.radius
    lazy val circleBottom = this.center.y - this.radius

    !(otherLeft > circleRight ||
      otherRight < circleLeft ||
      otherBottom > circleTop ||
      otherTop < circleBottom)
  }

  def intersects(rect: Rectangle): Boolean = {

    lazy val rectLeft = rect.left
    lazy val rectRight = rect.right
    lazy val rectTop = rect.top
    lazy val rectBottom = rect.bottom

    intersects(rectLeft, rectBottom, rectRight, rectTop)
  }

  def expandRadius(expandBy: Double): Unit =
    radius += expandBy

  override def toString: String =
    "%,.8f,%,.8f,%,.8f".format(center.x, center.y, radius)
}