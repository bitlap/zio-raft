package com.ariskk.raft

import scala.util.Random

import zio._
import zio.stm._
import zio.clock._
import zio.duration._

import com.ariskk.raft.model._
import Message._

/**
 * Relays messages between Raft nodes to allow for quick in-memory leader election
 * and command submission testing.
 * By passing `chaos = true`, one can emulate a faulty network where
 * messages are reordered, dropped and delayed arbitrarily.
 * The implementation is non-deterministic on purpose as the algorithm must
 * converge at all times.
 */
final class TestCluster[T](nodeRef: TRef[Seq[Raft[T]]], chaos: Boolean) {

  def getNode(id: NodeId) = for {
    nodes <- nodeRef.get.commit
    node  <- ZIO.fromOption(nodes.find(_.nodeId == id))
  } yield node

  def getNodes: UIO[Seq[Raft[T]]] = nodeRef.get.commit

  def getNodeStates: UIO[Iterable[NodeState]] = for {
    nodes    <- nodeRef.get.commit
    nodeData <- ZIO.collectAll(nodes.map(_.nodeState))
  } yield nodeData

  def addNewPeer: RIO[Clock, NodeId] = for {
    nodes   <- nodeRef.get.commit
    newNode <- TestRaft[T](NodeId.newUniqueId, nodes.map(_.nodeId).toSet)
    _       <- ZIO.collectAll(nodes.map(_.addPeer(newNode.nodeId)))
    _       <- newNode.run.fork
    _       <- nodeRef.update(_ :+ newNode).commit
  } yield newNode.nodeId

  private def sendMessage(m: Message) = for {
    node <- getNode(m.to)
    _ <- m match {
      case v: VoteRequest           => node.offerVoteRequest(v)
      case v: VoteResponse          => node.offerVote(v)
      case ae: AppendEntries[T]     => node.offerAppendEntries(ae)
      case r: AppendEntriesResponse => node.offerAppendEntriesResponse(r)
      case _                        => ZIO.die(new UnsupportedOperationException("Message type not supported"))
    }
  } yield ()

  /**
   * Simulates a shitty network.
   * Your network might not be better than this.
   */
  private def networkChaos(messages: Iterable[Message]) = {
    val shuffled   = scala.util.Random.shuffle(messages)
    val ios        = shuffled.map(sendMessage)
    val delayedIOs = ios.map(_.delay(Random.nextInt(5).milliseconds))
    if (Random.nextInt(10) > 8) delayedIOs.dropRight(1) else delayedIOs
  }

  def run = {

    lazy val startNodes = for {
      nodes <- nodeRef.get.commit
      _ <- ZIO.collectAllPar_(
        nodes.map(_.run)
      )
    } yield ()

    lazy val program = for {
      nodes   <- nodeRef.get.commit
      allMsgs <- ZIO.collectAll(nodes.map(_.takeAll)).map(_.flatten)
      msgIOs = if (chaos) networkChaos(allMsgs) else allMsgs.map(sendMessage)
      _ <- ZIO.collectAll(msgIOs)
    } yield ()

    startNodes <&> program.forever

  }
}

object TestCluster {

  def apply[T](numberOfNodes: Int, chaos: Boolean = false): UIO[TestCluster[T]] = {
    val nodeIds = (1 to numberOfNodes).map(_ => NodeId.newUniqueId).toSet

    for {
      nodes   <- ZIO.collectAll(nodeIds.map(id => TestRaft[T](id, nodeIds - id)))
      nodeRef <- TRef.makeCommit(nodes.toSeq)
    } yield new TestCluster[T](nodeRef, chaos)
  }

  def applyUnit(numberOfNodes: Int, chaos: Boolean = false): UIO[TestCluster[Unit]] =
    apply[Unit](numberOfNodes, chaos)
}
