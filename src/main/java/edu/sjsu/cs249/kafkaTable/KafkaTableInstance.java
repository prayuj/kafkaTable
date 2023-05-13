package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Semaphore;

public class KafkaTableInstance {
    public final String bootstrapServer;
    public final String name;
    private final int port;
    public final int snapshotCycle;
    public final String OPERATIONS_TOPIC;
    private final String SNAPSHOT_TOPIC;
    private final String SNAPSHOT_ORDERING_TOPIC;
    public long snapshotOrderingOffset;
    public long operationsOffset;
    public String snapshotReplicaId;
    HashMap<ClientXid, StreamObserver<IncResponse>> pendingIncRequests;
    HashMap<ClientXid, StreamObserver<GetResponse>> pendingGetRequests;
    HashMap<String, Integer> clientCounters;
    Hashtable<String, Integer> hashtable;
    KafkaConsumer<String, byte[]> operationsConsumer;
    KafkaConsumer<String, byte[]> snapshotOrderingConsumer;
    KafkaConsumer<String, byte[]> snapshotConsumer;

    public final Semaphore operationsSemaphore = new Semaphore(1);

    public KafkaTableInstance(String server, String name, int port, int snapshotCycle, String topicPrefix) {
        this.bootstrapServer = server;
        this.name = name;
        this.port = port;
        this.snapshotCycle = snapshotCycle;
        OPERATIONS_TOPIC = topicPrefix + "operations";
        SNAPSHOT_TOPIC = topicPrefix + "snapshot";
        SNAPSHOT_ORDERING_TOPIC = topicPrefix + "snapshotOrdering";
        snapshotOrderingOffset = -1;
        operationsOffset = -1;
        snapshotReplicaId = "";
        hashtable = new Hashtable<>();
        pendingIncRequests = new HashMap<>();
        pendingGetRequests = new HashMap<>();
        clientCounters = new HashMap<>();
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
            try {
                addLog("Trying to acquire consumer semaphore");
                operationsSemaphore.acquire();
                operationsConsumer.unsubscribe();
            } catch (InterruptedException e) {
                addLog("Problem acquiring semaphore while shutting down");
            }
            snapshotOrderingConsumer.unsubscribe();
            snapshotConsumer.unsubscribe();
            server.shutdown();
            addLog("Successfully stopped the server");
        }));

        /**
         *  1) insert itself into the snapshotOrdering topic if it isn't already there
         *  2) grab the latest snapshot from the snapshot topic and
         *  3) start consuming from the last offset in the snapshot.
         * */

        snapshotConsumer = createConsumer(SNAPSHOT_TOPIC, 0L, "snapshot");
        grabLatestSnapshot();

        snapshotOrderingConsumer = createConsumer(SNAPSHOT_ORDERING_TOPIC, snapshotOrderingOffset + 1, "snapshotOrdering");
        addToSnapshotOrdering();
        Thread polling = new Thread(new PollingTopics(this), "polling");
        polling.start();
        server.awaitTermination();
    }

    private void addToSnapshotOrdering() {
        var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
        for (var record: records) {
            try {
                var message = SnapshotOrdering.parseFrom(record.value());
                if (message.getReplicaId().equals(name)) {
                    return;
                }
            } catch (InvalidProtocolBufferException e) {
                addLog("Unable to parse value: " + e);
            }
        }
        publishToTopic(SNAPSHOT_ORDERING_TOPIC,
                SnapshotOrdering.newBuilder().setReplicaId(name).build().toByteArray());
    }

    private void grabLatestSnapshot() {
        var records = snapshotConsumer.poll(Duration.ofSeconds(1));
        for (var record: records) {
            try {
                var message = Snapshot.parseFrom(record.value());
                snapshotReplicaId = message.getReplicaId();
                for (String key: message.getTableMap().keySet()) {
                    hashtable.put(key, message.getTableMap().get(key));
                }
                operationsOffset = message.getOperationsOffset();
                for (String key: message.getClientCountersMap().keySet()) {
                    clientCounters.put(key, message.getClientCountersMap().get(key));
                }
                snapshotOrderingOffset = message.getSnapshotOrderingOffset();
                addLog("Snapshot consumed: " + message);
            } catch (InvalidProtocolBufferException e) {
                addLog("Unable to parse value: " + e);
            }
        }
    }

    public void publishToTopic(String topicName, byte[] bytes) {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var record = new ProducerRecord<String, byte[]>(topicName, bytes);
        Thread t = new Thread(() -> {
            producer.send(record);
            addLog("Published to " + topicName);
        });
        t.start();
    }
    public void addLog(Object message) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp + " " + message);
    }

    public void takeSnapshot() throws InterruptedException {
        /**
         *
         *  it will consume a replica name from snapshotOrdering.
         *  if it pulls its own name, it will publish a snapshot and reinsert its name into snapshotOrdering.
         *  (hmm, we have a consumer offset synchronization problem that we should discuss in class.)
         * */
        snapshotOrderingConsumer.seek(new TopicPartition(SNAPSHOT_ORDERING_TOPIC, 0), snapshotOrderingOffset + 1);
        snapshotOrderingConsumer.poll(Duration.ZERO);
        var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
        for (var record: records) {
            addLog("Polling Snapshot Ordering");
            try {
                SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                snapshotOrderingOffset = record.offset();
                addLog("Current name pulled: " + message.getReplicaId());
                if (message.getReplicaId().equals(this.name)) {
                    addLog("Time to publish snapshot");
                    var bytes = Snapshot.newBuilder()
                            .setReplicaId(name)
                            .putAllTable(hashtable)
                            .setOperationsOffset(operationsOffset)
                            .putAllClientCounters(clientCounters)
                            .setSnapshotOrderingOffset(snapshotOrderingOffset)
                            .build().toByteArray();
                    publishToTopic(SNAPSHOT_TOPIC, bytes);
                    publishToTopic(SNAPSHOT_ORDERING_TOPIC,
                            SnapshotOrdering.newBuilder().setReplicaId(name).build().toByteArray());
                }
            } catch (InvalidProtocolBufferException e) {
                addLog("Unable to parse value: " + e);
            }
            break;
        }
    }

    public KafkaConsumer<String, byte[]> createConsumer(String topic, long offset, String suffix) throws InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, name + suffix);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        addLog("Starting at " + new Date());
        var sem = new Semaphore(0);
        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                addLog("Didn't expect the revoke!");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                addLog("Partition assigned");
                collection.stream().forEach(t -> consumer.seek(t, offset));
                sem.release();
            }
        });
        addLog("first poll count: " + consumer.poll(0).count());
        sem.acquire();
        return consumer;
    }

    public boolean isValidRequest(String clientId, int incomingCounter) {
        return !clientCounters.containsKey(clientId) ||
                clientCounters.get(clientId) < incomingCounter;
    }
}

class PollingTopics extends Thread {

    KafkaTableInstance kt;
    PollingTopics(KafkaTableInstance kt) {
        this.kt = kt;
    }
    @Override
    public void run() {
        kt.addLog("Starting polling thread");
        try {
            kt.operationsConsumer = kt.createConsumer(kt.OPERATIONS_TOPIC, kt.operationsOffset + 1, "operations");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        while (true) {
            synchronized (kt) {
                try {
                    kt.operationsSemaphore.acquire();
                    kt.addLog("Polling");
                    var records = kt.operationsConsumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        kt.addLog("Received message!");
                        kt.addLog(record.headers());
                        kt.addLog(record.timestamp());
                        kt.addLog(record.timestampType());
                        long offset = record.offset();
                        kt.addLog(offset);
                        kt.operationsOffset = offset;
                        PublishedItem message = null;
                        try {
                            message = PublishedItem.parseFrom(record.value());
                            kt.addLog(message);
                            if (message.hasInc()) {
                                IncRequest incRequest = message.getInc();

                                // respond to client if you have this message in your queue
                                if (kt.pendingIncRequests.containsKey(incRequest.getXid())) {
                                    StreamObserver<IncResponse> responseObserver = kt.pendingIncRequests.remove(incRequest.getXid());
                                    responseObserver.onNext(IncResponse.newBuilder().build());
                                    responseObserver.onCompleted();
                                }

                                if (!kt.isValidRequest(incRequest.getXid().getClientid(), incRequest.getXid().getCounter())) {
                                    kt.addLog("Duplicate inc publish message, ...ignoring");
                                    continue;
                                }

                                String key = incRequest.getKey();
                                int incValue = incRequest.getIncValue();

                                kt.clientCounters.put(incRequest.getXid().getClientid(), incRequest.getXid().getCounter());

                                if (kt.hashtable.get(key) != null && kt.hashtable.get(key) + incValue >= 0) {
                                    kt.hashtable.put(key, kt.hashtable.get(key) + incValue);
                                } else if (incValue >= 0) {
                                    kt.hashtable.put(key, incValue);
                                }
                            } else {
                                GetRequest getRequest = message.getGet();

                                if (!kt.isValidRequest(getRequest.getXid().getClientid(), getRequest.getXid().getCounter())) {
                                    kt.addLog("Duplicate get publish message, ...ignoring");

                                    if (kt.pendingGetRequests.containsKey(getRequest.getXid())) {
                                        StreamObserver<GetResponse> responseObserver = kt.pendingGetRequests.get(getRequest.getXid());
                                        responseObserver.onNext(GetResponse.newBuilder().build());
                                        responseObserver.onCompleted();
                                    }
                                    continue;
                                }

                                kt.clientCounters.put(getRequest.getXid().getClientid(), getRequest.getXid().getCounter());
                                if (kt.pendingGetRequests.containsKey(getRequest.getXid())) {
                                    String key = getRequest.getKey();
                                    StreamObserver<GetResponse> responseObserver = kt.pendingGetRequests.get(getRequest.getXid());
                                    int value = kt.hashtable.get(key) != null ? kt.hashtable.get(key) : 0;
                                    responseObserver.onNext(GetResponse.newBuilder().setValue(value).build());
                                    responseObserver.onCompleted();
                                }
                            }
                            if (offset % kt.snapshotCycle == 0) {
                                kt.addLog("Triggered Snapshot Cycle!");
                                kt.takeSnapshot();
                            }
                        } catch (InvalidProtocolBufferException e) {
                            kt.addLog("Unable to parse value: " + e);
                        } catch (Exception e) {
                            kt.addLog("Exception in message parsing block" + e);
                        }

                    }
                    kt.operationsSemaphore.release();
                    kt.addLog("Released semaphore");
                } catch (InterruptedException e) {
                    kt.addLog("Problem acquiring semaphore");
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
            kt.addLog("Inc GRPC called");
            // If the request has already been seen ignore it
            if(!kt.isValidRequest(request.getXid().getClientid(), request.getXid().getCounter())){
                kt.addLog("Duplicate Request, ...ignoring");
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
            kt.addLog("Get GRPC called");
            // If the request has already been seen ignore it
            if (!kt.isValidRequest(request.getXid().getClientid(), request.getXid().getCounter())) {
                kt.addLog("Duplicate Request, ...ignoring");
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
        synchronized (kt) {
            kt.addLog("Debug GRPC called");
            Snapshot snapshot = Snapshot.newBuilder()
                    .setReplicaId(kt.name)
                    .putAllTable(kt.hashtable)
                    .setOperationsOffset(kt.operationsOffset)
                    .putAllClientCounters(kt.clientCounters)
                    .setSnapshotOrderingOffset(kt.snapshotOrderingOffset).build();
            responseObserver.onNext(KafkaTableDebugResponse.newBuilder()
                    .setSnapshot(snapshot)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        kt.addLog("Exit GRPC called");
        responseObserver.onNext(ExitResponse.newBuilder().build());
        responseObserver.onCompleted();
        System.exit(0);
    }
}
