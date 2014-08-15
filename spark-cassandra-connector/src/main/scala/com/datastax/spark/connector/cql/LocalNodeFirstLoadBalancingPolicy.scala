package com.datastax.spark.connector.cql

import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.{Statement, Cluster, HostDistance, Host}
import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConversions._
import scala.util.Random
import org.apache.spark.{Logging}

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs. */
class LocalNodeFirstLoadBalancingPolicy(contactPoints: Set[InetAddress]) extends LoadBalancingPolicy with Logging {

  import LocalNodeFirstLoadBalancingPolicy._

  logInfo(s"policy contactPoints: " + contactPoints.mkString("\n"))

  private var liveNodes = Set.empty[Host]
  private val random = new Random

  override def distance(host: Host): HostDistance = {
    val ret = if (isLocalHost(host))
      HostDistance.LOCAL
    else
      HostDistance.REMOTE
    logInfo(s"distance for host ${host} is ${ret}")
    ret
  }

  override def init(cluster: Cluster, hosts: java.util.Collection[Host]) {
    liveNodes = hosts.filter(_.isUp).toSet
    logInfo(s"after init liveNodes is " + liveNodes.mkString("\n"))
  }

  override def newQueryPlan(query: String, statement: Statement): java.util.Iterator[Host] = {
    logInfo(s"""contactPoints ${contactPoints.mkString("\n")} liveNodes ${liveNodes.mkString("\n")}""")
    val nodesInLocalDC = CassandraConnector.nodesInTheSameDC(contactPoints, liveNodes)
    val (localHost, otherHosts) = nodesInLocalDC.partition(isLocalHost)
    logInfo(s"""localHost ${localHost.mkString("\n")} otherHosts ${otherHosts.mkString("\n")}""")
    val plan = (localHost.toSeq ++ random.shuffle(otherHosts.toSeq))
    logInfo(s"query plan for ${query} ${statement} is " + plan.mkString("\n"))
    plan.iterator
  }

  override def onAdd(host: Host) {
    if (host.isUp) {
      // The added host might be a "better" version of a host already in the set.
      // The nodes added in the init call don't have DC and rack set.
      // Therefore we want to really replace the object now, to get full information on DC:
      liveNodes -= host
      liveNodes += host
    }
  }

  override def onRemove(host: Host) { liveNodes -= host }
  override def onUp(host: Host) = { liveNodes += host }
  override def onDown(host: Host) = { liveNodes -= host }
}

object LocalNodeFirstLoadBalancingPolicy {

  private val localAddresses =
    NetworkInterface.getNetworkInterfaces.flatMap(_.getInetAddresses).toSet

  def isLocalHost(host: Host): Boolean = {
    val hostAddress = host.getAddress
    hostAddress.isLoopbackAddress || localAddresses.contains(hostAddress)
  }
}
