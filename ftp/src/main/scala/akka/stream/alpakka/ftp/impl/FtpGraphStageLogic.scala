/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.stage.GraphStageLogic
import akka.stream.{Inlet, Outlet, Shape}

import scala.util.control.NonFatal

private[ftp] abstract class FtpGraphStageLogic[T, FtpClient, S <: RemoteFileSettings](
    val shape: Shape,
    val ftpLike: FtpLike[FtpClient, S],
    val connectionSettings: S,
    val ftpClient: () => FtpClient
) extends GraphStageLogic(shape) {

  protected[this] implicit val client = ftpClient()
  protected[this] val out: Outlet[T] = shape.outlets.length match {
    case 0 => Outlet.create("none")
    case _ => shape.outlets.head.asInstanceOf[Outlet[T]]
  }
  protected[this] val in: Inlet[T] = shape.inlets.length match {
    case 0 => Inlet.create("none")
    case _ => shape.inlets.head.asInstanceOf[Inlet[T]]
  }
  protected[this] var handler: Option[ftpLike.Handler] = Option.empty[ftpLike.Handler]
  protected[this] var isConnected: Boolean = false

  override def preStart(): Unit = {
    super.preStart()
    try {
      val tryConnect = ftpLike.connect(connectionSettings)
      if (tryConnect.isSuccess) {
        handler = tryConnect.toOption
        isConnected = true
      } else
        tryConnect.failed.foreach { case NonFatal(t) => throw t }
      doPreStart()
    } catch {
      case NonFatal(t) =>
        disconnect()
        matFailure(t)
        failStage(t)
    }
  }

  override def postStop(): Unit = {
    disconnect()
    matSuccess()
    super.postStop()
  }

  protected[this] def doPreStart(): Unit

  protected[this] def disconnect(): Unit =
    if (isConnected) {
      ftpLike.disconnect(handler.get)
      isConnected = false
    }

  protected[this] def matSuccess(): Boolean

  protected[this] def matFailure(t: Throwable): Boolean

}
