package com.custombroker.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class QueueServiceGrpc {

  private QueueServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "broker.QueueService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.custombroker.grpc.DeclareQueueRequest,
      com.custombroker.grpc.QueueOperationResponse> getDeclareQueueMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeclareQueue",
      requestType = com.custombroker.grpc.DeclareQueueRequest.class,
      responseType = com.custombroker.grpc.QueueOperationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.custombroker.grpc.DeclareQueueRequest,
      com.custombroker.grpc.QueueOperationResponse> getDeclareQueueMethod() {
    io.grpc.MethodDescriptor<com.custombroker.grpc.DeclareQueueRequest, com.custombroker.grpc.QueueOperationResponse> getDeclareQueueMethod;
    if ((getDeclareQueueMethod = QueueServiceGrpc.getDeclareQueueMethod) == null) {
      synchronized (QueueServiceGrpc.class) {
        if ((getDeclareQueueMethod = QueueServiceGrpc.getDeclareQueueMethod) == null) {
          QueueServiceGrpc.getDeclareQueueMethod = getDeclareQueueMethod =
              io.grpc.MethodDescriptor.<com.custombroker.grpc.DeclareQueueRequest, com.custombroker.grpc.QueueOperationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeclareQueue"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.DeclareQueueRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.QueueOperationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueueServiceMethodDescriptorSupplier("DeclareQueue"))
              .build();
        }
      }
    }
    return getDeclareQueueMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.custombroker.grpc.DeleteQueueRequest,
      com.custombroker.grpc.QueueOperationResponse> getDeleteQueueMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteQueue",
      requestType = com.custombroker.grpc.DeleteQueueRequest.class,
      responseType = com.custombroker.grpc.QueueOperationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.custombroker.grpc.DeleteQueueRequest,
      com.custombroker.grpc.QueueOperationResponse> getDeleteQueueMethod() {
    io.grpc.MethodDescriptor<com.custombroker.grpc.DeleteQueueRequest, com.custombroker.grpc.QueueOperationResponse> getDeleteQueueMethod;
    if ((getDeleteQueueMethod = QueueServiceGrpc.getDeleteQueueMethod) == null) {
      synchronized (QueueServiceGrpc.class) {
        if ((getDeleteQueueMethod = QueueServiceGrpc.getDeleteQueueMethod) == null) {
          QueueServiceGrpc.getDeleteQueueMethod = getDeleteQueueMethod =
              io.grpc.MethodDescriptor.<com.custombroker.grpc.DeleteQueueRequest, com.custombroker.grpc.QueueOperationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteQueue"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.DeleteQueueRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.custombroker.grpc.QueueOperationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new QueueServiceMethodDescriptorSupplier("DeleteQueue"))
              .build();
        }
      }
    }
    return getDeleteQueueMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static QueueServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueueServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueueServiceStub>() {
        @java.lang.Override
        public QueueServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueueServiceStub(channel, callOptions);
        }
      };
    return QueueServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static QueueServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueueServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueueServiceBlockingV2Stub>() {
        @java.lang.Override
        public QueueServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueueServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return QueueServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static QueueServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueueServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueueServiceBlockingStub>() {
        @java.lang.Override
        public QueueServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueueServiceBlockingStub(channel, callOptions);
        }
      };
    return QueueServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static QueueServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QueueServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<QueueServiceFutureStub>() {
        @java.lang.Override
        public QueueServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new QueueServiceFutureStub(channel, callOptions);
        }
      };
    return QueueServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void declareQueue(com.custombroker.grpc.DeclareQueueRequest request,
        io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeclareQueueMethod(), responseObserver);
    }

    /**
     */
    default void deleteQueue(com.custombroker.grpc.DeleteQueueRequest request,
        io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteQueueMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service QueueService.
   */
  public static abstract class QueueServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return QueueServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service QueueService.
   */
  public static final class QueueServiceStub
      extends io.grpc.stub.AbstractAsyncStub<QueueServiceStub> {
    private QueueServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueueServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueueServiceStub(channel, callOptions);
    }

    /**
     */
    public void declareQueue(com.custombroker.grpc.DeclareQueueRequest request,
        io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeclareQueueMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteQueue(com.custombroker.grpc.DeleteQueueRequest request,
        io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteQueueMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service QueueService.
   */
  public static final class QueueServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<QueueServiceBlockingV2Stub> {
    private QueueServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueueServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueueServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public com.custombroker.grpc.QueueOperationResponse declareQueue(com.custombroker.grpc.DeclareQueueRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeclareQueueMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.custombroker.grpc.QueueOperationResponse deleteQueue(com.custombroker.grpc.DeleteQueueRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteQueueMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service QueueService.
   */
  public static final class QueueServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<QueueServiceBlockingStub> {
    private QueueServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueueServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueueServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.custombroker.grpc.QueueOperationResponse declareQueue(com.custombroker.grpc.DeclareQueueRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeclareQueueMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.custombroker.grpc.QueueOperationResponse deleteQueue(com.custombroker.grpc.DeleteQueueRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteQueueMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service QueueService.
   */
  public static final class QueueServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<QueueServiceFutureStub> {
    private QueueServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QueueServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QueueServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.custombroker.grpc.QueueOperationResponse> declareQueue(
        com.custombroker.grpc.DeclareQueueRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeclareQueueMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.custombroker.grpc.QueueOperationResponse> deleteQueue(
        com.custombroker.grpc.DeleteQueueRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteQueueMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DECLARE_QUEUE = 0;
  private static final int METHODID_DELETE_QUEUE = 1;

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
        case METHODID_DECLARE_QUEUE:
          serviceImpl.declareQueue((com.custombroker.grpc.DeclareQueueRequest) request,
              (io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse>) responseObserver);
          break;
        case METHODID_DELETE_QUEUE:
          serviceImpl.deleteQueue((com.custombroker.grpc.DeleteQueueRequest) request,
              (io.grpc.stub.StreamObserver<com.custombroker.grpc.QueueOperationResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getDeclareQueueMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.custombroker.grpc.DeclareQueueRequest,
              com.custombroker.grpc.QueueOperationResponse>(
                service, METHODID_DECLARE_QUEUE)))
        .addMethod(
          getDeleteQueueMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.custombroker.grpc.DeleteQueueRequest,
              com.custombroker.grpc.QueueOperationResponse>(
                service, METHODID_DELETE_QUEUE)))
        .build();
  }

  private static abstract class QueueServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    QueueServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.custombroker.grpc.BrokerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("QueueService");
    }
  }

  private static final class QueueServiceFileDescriptorSupplier
      extends QueueServiceBaseDescriptorSupplier {
    QueueServiceFileDescriptorSupplier() {}
  }

  private static final class QueueServiceMethodDescriptorSupplier
      extends QueueServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    QueueServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (QueueServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new QueueServiceFileDescriptorSupplier())
              .addMethod(getDeclareQueueMethod())
              .addMethod(getDeleteQueueMethod())
              .build();
        }
      }
    }
    return result;
  }
}
