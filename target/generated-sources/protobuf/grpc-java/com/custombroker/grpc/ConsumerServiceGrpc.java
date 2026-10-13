package com.custombroker.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class ConsumerServiceGrpc {

  private ConsumerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "broker.ConsumerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.custombroker.grpc.ConsumerRequest,
      com.custombroker.grpc.BrokerResponse> getOpenChannelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OpenChannel",
      requestType = com.custombroker.grpc.ConsumerRequest.class,
      responseType = com.custombroker.grpc.BrokerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.custombroker.grpc.ConsumerRequest,
      com.custombroker.grpc.BrokerResponse> getOpenChannelMethod() {
    io.grpc.MethodDescriptor<com.custombroker.grpc.ConsumerRequest, com.custombroker.grpc.BrokerResponse> getOpenChannelMethod;
    if ((getOpenChannelMethod = ConsumerServiceGrpc.getOpenChannelMethod) == null) {
      synchronized (ConsumerServiceGrpc.class) {
        if ((getOpenChannelMethod = ConsumerServiceGrpc.getOpenChannelMethod) == null) {
          ConsumerServiceGrpc.getOpenChannelMethod = getOpenChannelMethod =
              io.grpc.MethodDescriptor.<com.custombroker.grpc.ConsumerRequest, com.custombroker.grpc.BrokerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenChannel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.ConsumerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.BrokerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerServiceMethodDescriptorSupplier("OpenChannel"))
              .build();
        }
      }
    }
    return getOpenChannelMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ConsumerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceStub>() {
        @java.lang.Override
        public ConsumerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceStub(channel, callOptions);
        }
      };
    return ConsumerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static ConsumerServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingV2Stub>() {
        @java.lang.Override
        public ConsumerServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return ConsumerServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ConsumerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingStub>() {
        @java.lang.Override
        public ConsumerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceBlockingStub(channel, callOptions);
        }
      };
    return ConsumerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ConsumerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceFutureStub>() {
        @java.lang.Override
        public ConsumerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceFutureStub(channel, callOptions);
        }
      };
    return ConsumerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<com.custombroker.grpc.ConsumerRequest> openChannel(
        io.grpc.stub.StreamObserver<com.custombroker.grpc.BrokerResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getOpenChannelMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ConsumerService.
   */
  public static abstract class ConsumerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ConsumerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ConsumerServiceStub> {
    private ConsumerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.custombroker.grpc.ConsumerRequest> openChannel(
        io.grpc.stub.StreamObserver<com.custombroker.grpc.BrokerResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getOpenChannelMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<ConsumerServiceBlockingV2Stub> {
    private ConsumerServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.custombroker.grpc.ConsumerRequest, com.custombroker.grpc.BrokerResponse>
        openChannel() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getOpenChannelMethod(), getCallOptions());
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ConsumerServiceBlockingStub> {
    private ConsumerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ConsumerServiceFutureStub> {
    private ConsumerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_OPEN_CHANNEL = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_OPEN_CHANNEL:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.openChannel(
              (io.grpc.stub.StreamObserver<com.custombroker.grpc.BrokerResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getOpenChannelMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.custombroker.grpc.ConsumerRequest,
              com.custombroker.grpc.BrokerResponse>(
                service, METHODID_OPEN_CHANNEL)))
        .build();
  }

  private static abstract class ConsumerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ConsumerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.custombroker.grpc.BrokerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ConsumerService");
    }
  }

  private static final class ConsumerServiceFileDescriptorSupplier
      extends ConsumerServiceBaseDescriptorSupplier {
    ConsumerServiceFileDescriptorSupplier() {}
  }

  private static final class ConsumerServiceMethodDescriptorSupplier
      extends ConsumerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ConsumerServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ConsumerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ConsumerServiceFileDescriptorSupplier())
              .addMethod(getOpenChannelMethod())
              .build();
        }
      }
    }
    return result;
  }
}
