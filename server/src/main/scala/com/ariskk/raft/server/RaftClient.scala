package com.ariskk.raft.server

import java.io.IOException

import scala.reflect.ClassTag
import scala.util.Random

import zio._
import zio.clock.Clock
import zio.duration._
import zio.nio.channels._
import zio.nio.{ InetAddress, InetSocketAddress }

import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.ariskk.raft.model.CommandResponse._
import com.ariskk.raft.model._

final class RaftClient(
  nodes: Seq[RaftServer.Config],
  serdeRef: Ref[Serde],
  leaderRef: Ref[RaftServer.Config]
) {

  private val chunkSize: Int = 1000

  private val backOff = Seq(
    10.milliseconds,
    50.milliseconds,
    200.milliseconds,
    1000.milliseconds
  )

  private def shuffleLeaderRef = leaderRef.set(nodes(Random.nextInt(nodes.size)))

  def channel(address: InetAddress, port: Int): ZManaged[Any with Clock, IOException, AsynchronousSocketChannel] =
    AsynchronousSocketChannel.open.mapM { client =>
      for {
        address <- InetSocketAddress.inetAddress(address, port)
        _ <- client
          .connect(address)
          .retry(Schedule.fromDurations(5.milliseconds, backOff: _*))
      } yield client
    }

  private def lookupLeaderThenSubmit[T: ClassTag](command: Command): ZIO[Clock, Exception, CommandResponse] =
    for {
      serde  <- serdeRef.get
      leader <- leaderRef.get
      client = channel(leader.address, leader.raftClientPort)
      chunks <- client.use(c =>
        c.writeChunk(Chunk.fromArray(serde.serialize(command))) *>
          c.readChunk(chunkSize)
      )
      response <- ZIO.fromEither(serde.deserialize[CommandResponse](chunks.toArray))
      _ <- response match {
        case Committed | QueryResult(_) => ZIO.unit
        case LeaderNotFoundResponse =>
          shuffleLeaderRef *> lookupLeaderThenSubmit[T](command)
        case Redirect(leaderId) =>
          for {
            newLeader <- ZIO
              .fromOption(nodes.find(_.nodeId == leaderId))
              .mapError(_ => new Exception("Failed to find leader in node config"))
            _ <- leaderRef.set(newLeader)
            _ <- lookupLeaderThenSubmit[T](command)
          } yield ()
      }
    } yield response

  def submitCommand[T: ClassTag](command: WriteCommand): ZIO[Clock, Exception, CommandResponse] =
    lookupLeaderThenSubmit[T](command)

  def submitQuery[T: ClassTag](query: ReadCommand): ZIO[Clock, Exception, Option[T]] =
    lookupLeaderThenSubmit[T](query).map {
      case QueryResult(data: Option[T]) => data
      case _                            => None
    }

}

object RaftClient {
  def apply(nodes: Seq[RaftServer.Config]): IO[Option[Nothing], RaftClient] = for {
    serdeRef  <- Ref.make(Serde.kryo)
    leader    <- ZIO.fromOption(nodes.headOption)
    leaderRef <- Ref.make(leader)
  } yield new RaftClient(nodes, serdeRef, leaderRef)
}
