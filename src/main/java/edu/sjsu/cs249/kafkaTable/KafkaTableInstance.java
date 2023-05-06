package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class KafkaTableInstance {
    public final String bootstrapServer;
    public final String name;
    private final int port;
    public final int snapshotCycle;
    public final String OPERATIONS_TOPIC;
    private final String SNAPSHOT_TOPIC;
    private final String SNAPSHOT_ORDERING_TOPIC;
    HashMap<ClientXid, StreamObserver<IncResponse>> pendingIncRequests;
    HashMap<ClientXid, StreamObserver<GetResponse>> pendingGetRequests;
    HashMap<String, Integer> lastExecutedClient;
    Hashtable<String, Integer> hashtable;

    public KafkaTableInstance(String server, String name, int port, int snapshotCycle, String topicPrefix) {
        this.bootstrapServer = server;
        this.name = name;
        this.port = port;
        this.snapshotCycle = snapshotCycle;
        OPERATIONS_TOPIC = topicPrefix + "operations";
        SNAPSHOT_TOPIC = topicPrefix + "snapshot";
        SNAPSHOT_ORDERING_TOPIC = topicPrefix + "snapshotOrdering";
        hashtable = new Hashtable<>();
        pendingIncRequests = new HashMap<>();
        pendingGetRequests = new HashMap<>();
        lastExecutedClient = new HashMap<>();
    }

    public void start() throws IOException, InterruptedException {
        addLog("Starting up Server");
        Server server = ServerBuilder.forPort(port)
                .addService(new KafkaTableGrpcService(this))
                .addService(new KafkaTableDebugGrpcService(this))
                .build();
        server.start();
        addLog("Server listening on port: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            addLog("Successfully stopped the server");
        }));

        Thread polling = new Thread(new PollingTopics(this), "polling");
        polling.start();

        server.awaitTermination();
    }

    public void publishToTopic(String topicName, byte[] bytes) {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var record = new ProducerRecord<String, byte[]>(topicName, bytes);
        Thread t = new Thread(() -> {
            producer.send(record);
            addLog("Published!");
        });
        t.start();
    }
    public void addLog(Object message) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp + " " + message);
    }
}

class KafkaTableGrpcService extends KafkaTableGrpc.KafkaTableImplBase {
    final KafkaTableInstance kt;
    KafkaTableGrpcService (KafkaTableInstance kt) {
        this.kt = kt;
    }
    @Override
    public void inc(IncRequest request, StreamObserver<IncResponse> responseObserver) {
        synchronized (kt) {

            // If the request has already been seen ignore it
            if (kt.lastExecutedClient.containsKey(request.getXid().getClientid()) &&
                kt.lastExecutedClient.get(request.getXid().getClientid()) >= request.getXid().getCounter()) {
                responseObserver.onNext(IncResponse.newBuilder().build());
                responseObserver.onCompleted();
                return;
            }

            PublishedItem publishedItem = PublishedItem.newBuilder()
                    .setInc(request)
                    .build();
            kt.publishToTopic(kt.OPERATIONS_TOPIC, publishedItem.toByteArray());
            kt.pendingIncRequests.put(request.getXid(), responseObserver);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        synchronized (kt) {

            if (kt.lastExecutedClient.containsKey(request.getXid().getClientid()) &&
                    kt.lastExecutedClient.get(request.getXid().getClientid()) >= request.getXid().getCounter()) {
                responseObserver.onNext(GetResponse.newBuilder().build());
                responseObserver.onCompleted();
                return;
            }

            PublishedItem publishedItem = PublishedItem.newBuilder()
                    .setGet(request)
                    .build();
            kt.publishToTopic(kt.OPERATIONS_TOPIC, publishedItem.toByteArray());
            kt.pendingGetRequests.put(request.getXid(), responseObserver);
        }
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

class PollingTopics extends Thread {

    KafkaTableInstance kt;
    PollingTopics(KafkaTableInstance kt) {
        this.kt = kt;
    }
    @Override
    public void run() {
        this.kt.addLog("Starting polling thread");
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kt.bootstrapServer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.kt.name);
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(this.kt.OPERATIONS_TOPIC));
        while (true) {
            var records = consumer.poll(Duration.ofSeconds(1));
            for (var record: records) {
                this.kt.addLog("Received message!");
                this.kt.addLog(record.headers());
                this.kt.addLog(record.timestamp());
                this.kt.addLog(record.timestampType());
                long offset = record.offset();
                this.kt.addLog(offset);
                PublishedItem message = null;
                try {
                    message = PublishedItem.parseFrom(record.value());
                    this.kt.addLog(message);
                    synchronized (this.kt) {
                        if (message.hasInc()) {
                            IncRequest incRequest = message.getInc();
                            String key = incRequest.getKey();
                            int incValue = incRequest.getIncValue();

                            this.kt.lastExecutedClient.put(incRequest.getXid().getClientid(), incRequest.getXid().getCounter());

                            if (this.kt.hashtable.get(key) != null && this.kt.hashtable.get(key) + incValue >= 0) {
                                this.kt.hashtable.put(key, this.kt.hashtable.get(key) + incValue);
                            } else if (incValue >= 0) {
                                this.kt.hashtable.put(key, incValue);
                            }
                            if (this.kt.pendingIncRequests.containsKey(incRequest.getXid())) {
                                StreamObserver<IncResponse> responseObserver = this.kt.pendingIncRequests.get(incRequest.getXid());
                                responseObserver.onNext(IncResponse.newBuilder().build());
                                responseObserver.onCompleted();
                            }
                        } else {
                            GetRequest getRequest = message.getGet();
                            this.kt.lastExecutedClient.put(getRequest.getXid().getClientid(), getRequest.getXid().getCounter());
                            if (this.kt.pendingGetRequests.containsKey(getRequest.getXid())) {
                                String key = getRequest.getKey();
                                StreamObserver<GetResponse> responseObserver = this.kt.pendingGetRequests.get(getRequest.getXid());
                                int value = this.kt.hashtable.get(key) != null ? this.kt.hashtable.get(key) : 0;
                                responseObserver.onNext(GetResponse.newBuilder().setValue(value).build());
                                responseObserver.onCompleted();
                            }
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    this.kt.addLog("Unable to parse value");
                }

                if (offset % this.kt.snapshotCycle == 0) {
                    //TODO:
                }
            }
        }
    }
}
