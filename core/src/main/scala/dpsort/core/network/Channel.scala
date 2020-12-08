package dpsort.core.network

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}


trait Channel {
//  def channel: io.grpc.Channel
//  val stub: Any
//  def request: Any
  def shutdown():Unit
}

// there might be better design for channel
// too many ovelapping code for each channels
