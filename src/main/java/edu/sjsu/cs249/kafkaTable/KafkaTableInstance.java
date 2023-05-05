package edu.sjsu.cs249.kafkaTable;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class KafkaTableInstance {
    private final String server;
    private final String name;
    private final int port;
    private final int snapshotCycle;
    private final String OPERATIONS_TOPIC;
    private final String SNAPSHOT_TOPIC;
    private final String SNAPSHOT_ORDERING_TOPIC;

    public KafkaTableInstance(String server, String name, int port, int snapshotCycle, String topicPrefix) {
        this.server = server;
        this.name = name;
        this.port = port;
        this.snapshotCycle = snapshotCycle;
        OPERATIONS_TOPIC = topicPrefix + "operations";
        SNAPSHOT_TOPIC = topicPrefix + "snapshot";
        SNAPSHOT_ORDERING_TOPIC = topicPrefix + "snapshotOrdering";
    }

    public void start() throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(port)
                .addService(new KafkaTableGrpcService(this))
                .addService(new KafkaTableDebugGrpcService(this))
                .build();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));
        server.awaitTermination();
    }
}

class KafkaTableGrpcService extends KafkaTableGrpc.KafkaTableImplBase {
    KafkaTableInstance kt;
    KafkaTableGrpcService (KafkaTableInstance kt) {
        this.kt = kt;
    }
    @Override
    public void inc(IncRequest request, StreamObserver<IncResponse> responseObserver) {

    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {

    }
}

class KafkaTableDebugGrpcService extends KafkaTableDebugGrpc.KafkaTableDebugImplBase {

    KafkaTableInstance kt;
    KafkaTableDebugGrpcService (KafkaTableInstance kt) {
        this.kt = kt;
    }
    @Override
    public void debug(KafkaTableDebugRequest request, StreamObserver<KafkaTableDebugResponse> responseObserver) {

    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {

    }
}
