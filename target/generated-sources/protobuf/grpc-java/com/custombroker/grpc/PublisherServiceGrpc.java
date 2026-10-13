package com.custombroker.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class PublisherServiceGrpc {

  private PublisherServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "broker.PublisherService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.custombroker.grpc.PublisherRequest,
      com.custombroker.grpc.PublisherResponse> getOpenPublisherChannelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OpenPublisherChannel",
      requestType = com.custombroker.grpc.PublisherRequest.class,
      responseType = com.custombroker.grpc.PublisherResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.custombroker.grpc.PublisherRequest,
      com.custombroker.grpc.PublisherResponse> getOpenPublisherChannelMethod() {
    io.grpc.MethodDescriptor<com.custombroker.grpc.PublisherRequest, com.custombroker.grpc.PublisherResponse> getOpenPublisherChannelMethod;
    if ((getOpenPublisherChannelMethod = PublisherServiceGrpc.getOpenPublisherChannelMethod) == null) {
      synchronized (PublisherServiceGrpc.class) {
        if ((getOpenPublisherChannelMethod = PublisherServiceGrpc.getOpenPublisherChannelMethod) == null) {
          PublisherServiceGrpc.getOpenPublisherChannelMethod = getOpenPublisherChannelMethod =
              io.grpc.MethodDescriptor.<com.custombroker.grpc.PublisherRequest, com.custombroker.grpc.PublisherResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenPublisherChannel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.PublisherRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.PublisherResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PublisherServiceMethodDescriptorSupplier("OpenPublisherChannel"))
              .build();
        }
      }
    }
    return getOpenPublisherChannelMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PublisherServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PublisherServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PublisherServiceStub>() {
        @java.lang.Override
        public PublisherServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PublisherServiceStub(channel, callOptions);
        }
      };
    return PublisherServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static PublisherServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PublisherServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PublisherServiceBlockingV2Stub>() {
        @java.lang.Override
        public PublisherServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PublisherServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return PublisherServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PublisherServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PublisherServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PublisherServiceBlockingStub>() {
        @java.lang.Override
        public PublisherServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PublisherServiceBlockingStub(channel, callOptions);
        }
      };
    return PublisherServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PublisherServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PublisherServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PublisherServiceFutureStub>() {
        @java.lang.Override
        public PublisherServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PublisherServiceFutureStub(channel, callOptions);
        }
      };
    return PublisherServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<com.custombroker.grpc.PublisherRequest> openPublisherChannel(
        io.grpc.stub.StreamObserver<com.custombroker.grpc.PublisherResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getOpenPublisherChannelMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service PublisherService.
   */
  public static abstract class PublisherServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return PublisherServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service PublisherService.
   */
  public static final class PublisherServiceStub
      extends io.grpc.stub.AbstractAsyncStub<PublisherServiceStub> {
    private PublisherServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PublisherServiceStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.custombroker.grpc.PublisherRequest> openPublisherChannel(
        io.grpc.stub.StreamObserver<com.custombroker.grpc.PublisherResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getOpenPublisherChannelMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service PublisherService.
   */
  public static final class PublisherServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<PublisherServiceBlockingV2Stub> {
    private PublisherServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PublisherServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.custombroker.grpc.PublisherRequest, com.custombroker.grpc.PublisherResponse>
        openPublisherChannel() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getOpenPublisherChannelMethod(), getCallOptions());
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service PublisherService.
   */
  public static final class PublisherServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<PublisherServiceBlockingStub> {
    private PublisherServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PublisherServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service PublisherService.
   */
  public static final class PublisherServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<PublisherServiceFutureStub> {
    private PublisherServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PublisherServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_OPEN_PUBLISHER_CHANNEL = 0;

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
        case METHODID_OPEN_PUBLISHER_CHANNEL:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.openPublisherChannel(
              (io.grpc.stub.StreamObserver<com.custombroker.grpc.PublisherResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getOpenPublisherChannelMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.custombroker.grpc.PublisherRequest,
              com.custombroker.grpc.PublisherResponse>(
                service, METHODID_OPEN_PUBLISHER_CHANNEL)))
        .build();
  }

  private static abstract class PublisherServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PublisherServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.custombroker.grpc.BrokerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PublisherService");
    }
  }

  private static final class PublisherServiceFileDescriptorSupplier
      extends PublisherServiceBaseDescriptorSupplier {
    PublisherServiceFileDescriptorSupplier() {}
  }

  private static final class PublisherServiceMethodDescriptorSupplier
      extends PublisherServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    PublisherServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (PublisherServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PublisherServiceFileDescriptorSupplier())
              .addMethod(getOpenPublisherChannelMethod())
              .build();
        }
      }
    }
    return result;
  }
}
