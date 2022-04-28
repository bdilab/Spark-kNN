package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.sknn.ds.util.PartitionerPointUserData
import org.cusp.bdi.sknn.util.Helper

class SparkKnnKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    Array(Helper.getClass,
      SortedLinkedList.getClass,
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[MBR],
      classOf[Geom2D],
      classOf[PartitionerPointUserData],
      classOf[KdTree],
      QuadTree.getClass,
      KdTree.getClass,
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      classOf[KdtLeafNode],
      classOf[SparkKnn],
      SparkKnn.getClass,
      SpatialIndex.getClass,
      classOf[SpatialIndex],
      classOf[Node[_]])
      .foreach(kryo.register)
  }
}
