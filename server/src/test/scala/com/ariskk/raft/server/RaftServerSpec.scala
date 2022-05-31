package com.ariskk.raft.server

import java.net.UnknownHostException
import zio.duration._
import zio.nio.InetAddress
import zio.test.Assertion._
import zio.test.environment._
import zio.test.{ DefaultRunnableSpec, _ }
import zio.{ Fiber, IO, ZIO }
import com.ariskk.raft.model._
import com.ariskk.raft.rocksdb._
import com.ariskk.raft.statemachine._

object RaftServerSpec extends DefaultRunnableSpec {

  override def aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(120.seconds))

  private lazy val serde: Serde = Serde.kryo

  private def createStorage(nodeId: NodeId): ZIO[Any, Throwable, RocksDBStorage] =
    RocksDBStorage.apply(s"/tmp/rocks-${nodeId.value}", "DB")

  private def createRaftServer[T](
    config: RaftServer.Config,
    peers: Set[RaftServer.Config]
  ): ZIO[Any, Throwable, RaftServer[T]] = for {
    storage      <- createStorage(config.nodeId)
    stateMachine <- KeyValueStore.apply[T]
    server       <- RaftServer(config, peers.toSeq, storage, stateMachine)
  } yield server

  private def createClient(nodes: Seq[RaftServer.Config]): IO[Option[Nothing], RaftClient] = RaftClient(nodes)

  private def generateConfig(serverPort: Int, clientPort: Int): ZIO[Any, UnknownHostException, RaftServer.Config] =
    InetAddress.localHost.map(a =>
      RaftServer.Config(
        NodeId.newUniqueId,
        a,
        raftPort = serverPort,
        raftClientPort = clientPort
      )
    )

  private def generateConfigs(numberOfNodes: Int): ZIO[Any, UnknownHostException, IndexedSeq[RaftServer.Config]] =
    ZIO.collectAll(
      (1 to numberOfNodes).map(i => generateConfig(19700 + i, 19800 + i))
    )

  private def electLeader(
    nodes: Int
  ): ZIO[Live, Serializable, (RaftClient, IndexedSeq[Fiber.Runtime[Exception, Unit]])] =
    for {
      configs <- generateConfigs(nodes)
      servers <- ZIO.collectAll(configs.map(c => createRaftServer[Int](c, configs.toSet - c)))
      client  <- createClient(configs)
      fibers  <- live(ZIO.collectAll(servers.map(_.run.fork)))
      _ <- ZIO.collectAll(servers.map(_.getState)).repeatUntil { states =>
        states.count(_ == NodeState.Leader) == 1 &&
        states.count(_ == NodeState.Follower) == nodes - 1
      }
    } yield client -> fibers

  def spec: Spec[TestEnvironment, TestFailure[Serializable], TestSuccess] =
    suite("RaftServerSpec")(
      testM("It should elect a leader, accept commands and respond to queries") {
        lazy val program = for {
          (client, fibers) <- electLeader(3)
          _                <- ZIO.collectAll((1 to 5).map(i => live(client.submitCommand(WriteKey(Key(s"key-$i"), i)))))
          _ <- ZIO.collectAll(
            (1 to 5).map(i =>
              client.submitQuery(ReadKey(Key(s"key-$i"))).repeatUntil { result: Option[_] =>
                result.contains(i)
              }
            )
          )
          _ <- ZIO.collectAll(fibers.map(_.interrupt))
        } yield ()

        assertM(program)(equalTo())
      },
      testM("It should elect a leader when has more nodes") {
        lazy val program = for {
          (client, fibers) <- electLeader(5)
          _                <- ZIO.collectAll((1 to 5).map(i => live(client.submitCommand(WriteKey(Key(s"key-$i"), i)))))
          _ <- ZIO.collectAll(
            (1 to 5).map(i =>
              client.submitQuery(ReadKey(Key(s"key-$i"))).repeatUntil { result: Option[_] =>
                result.contains(i)
              }
            )
          )
          _ <- ZIO.collectAll(fibers.map(_.interrupt))
        } yield ()
        assertM(program)(equalTo())
      }
    )

}
