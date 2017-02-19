/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.stage.{GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, IOResult, Inlet, SinkShape}
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.util.ByteString
import akka.util.ByteString.ByteString1C

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import java.io.OutputStream

private[ftp] trait FtpSinkStage[FtpClient, S <: RemoteFileSettings]
    extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[IOResult]] {

  def name: String

  def path: String

  def chunkSize: Int

  def connectionSettings: S

  implicit def ftpClient: () => FtpClient

  val ftpLike: FtpLike[FtpClient, S]

  override def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(name) and IODispatcher

  val shape = SinkShape(Inlet[ByteString](s"$name.in"))

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val matValuePromise = Promise[IOResult]()

    val logic = new FtpGraphStageLogic[ByteString, FtpClient, S](shape, ftpLike, connectionSettings, ftpClient) {

      private[this] var outstreamOpt: Option[OutputStream] = None
      private[this] var writeBytesTotal: Int = 0

      setHandler(in,
        new InHandler {
        def onPush(): Unit = {
          outstreamOpt.get.write(grab(in).decodeString("UTF-8").getBytes())
          tryPull()
        }

        override def onUpstreamFinish(): Unit =
          try {
            outstreamOpt.foreach(_.close())
            disconnect()
          } finally {
            matSuccess()
            super.onUpstreamFinish()
          }
        }
      ) // end of handler

      override def postStop(): Unit =
        try {
          outstreamOpt.foreach(_.close())
        } finally {
          super.postStop()
        }

      protected[this] def doPreStart(): Unit = {
        val outstreamTry = ftpLike.storeFileOutputStream(path, handler.get)
        if (outstreamTry.isSuccess)
          outstreamOpt = outstreamTry.toOption
        else
          outstreamTry.failed.foreach { case NonFatal(t) => throw t }

        pull(in)
      }

      private def tryPull(): Unit =
        if (!isClosed(in) && !hasBeenPulled(in)) {
          pull(in)
        }

      protected[this] def matSuccess(): Boolean =
        matValuePromise.trySuccess(IOResult.createSuccessful(writeBytesTotal))

      protected[this] def matFailure(t: Throwable): Boolean =
        matValuePromise.trySuccess(IOResult.createFailed(writeBytesTotal, t))

    } // end of stage logic

    (logic, matValuePromise.future)
  }

}
