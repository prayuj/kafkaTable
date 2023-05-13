package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Command
public class Main {
    static {
        // quiet some kafka messages
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }

    @Command
    int publish(@Parameters(paramLabel = "kafkaHost:port") String server,
                @Parameters(paramLabel = "topic-name") String name) throws IOException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var br = new BufferedReader(new InputStreamReader(System.in));
        for (int i = 0;; i++) {
            var line = br.readLine();
            if (line == null) break;
            var bytes = SimpleMessage.newBuilder()
                    .setMessage(line)
                    .build().toByteArray();
            var record = new ProducerRecord<String, byte[]>(name, bytes);
            producer.send(record);
        }
        return 0;
    }

    @Command
    int consume(@Parameters(paramLabel = "kafkaHost:port") String server,
                @Parameters(paramLabel = "topic-name") String name,
                @Parameters(paramLabel = "group-id") String id) throws InvalidProtocolBufferException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        System.out.println("Starting at " + new Date());
        var sem = new Semaphore(0);
        consumer.subscribe(List.of(name), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("Didn't expect the revoke!");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("Partition assigned");
                collection.stream().forEach(t -> consumer.seek(t, 0));
                sem.release();
            }
        });
        System.out.println("first poll count: " + consumer.poll(0).count());
        sem.acquire();
        System.out.println("Ready to consume at " + new Date());
        while (true) {
            var records = consumer.poll(Duration.ofSeconds(20));
            System.out.println("Got: " + records.count());
            for (var record: records) {
                System.out.println(record.headers());
                System.out.println(record.timestamp());
                System.out.println(record.timestampType());
                System.out.println(record.offset());
                if (name.contains("operations")) {
                    var message = PublishedItem.parseFrom(record.value());
                    System.out.println(message);
                } else if (name.contains("snapshotOrdering")) {
                    var message = SnapshotOrdering.parseFrom(record.value());
                    System.out.println(message);
                } else if (name.contains("snapshot")) {
                    var message = Snapshot.parseFrom(record.value());
                    System.out.println(message);
                } else {
                    var message = SimpleMessage.parseFrom(record.value());
                    System.out.println(message);
                }
            }
        }
    }

    @Command
    int listTopics(@Parameters(paramLabel = "kafkaHost:port") String server) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            var rc = admin.listTopics();
            var listings = rc.listings().get();
            for (var l : listings) {
                System.out.println(l);
            }
        }
        return 0;
    }

    @Command
    int createTopic(@Parameters(paramLabel = "kafkaHost:port") String server,
                    @Parameters(paramLabel = "topic-name") String name) throws InterruptedException, ExecutionException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            var rc = admin.createTopics(List.of(new NewTopic(name, 1, (short) 1)));
            rc.all().get();
        }
        return 0;
    }

    @Command(description = "delete the operations, snapshotOrder, and snapshot topics for a given prefix")
    int deleteTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                          @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            List<String> topics = List.of(
                    prefix + "operations",
                    prefix + "snapshot",
                    prefix + "snapshotOrdering"
            );
            admin.deleteTopics(topics);
            System.out.println("deleted topics: " + Arrays.toString(topics.toArray()));
        }
        return 0;
    }
    @Command(description = "create the operations, snapshotOrder, and snapshot topics for a given prefix")
    int createTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                          @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            var rc = admin.createTopics(List.of(
                    new NewTopic(prefix + "operations", 1, (short) 1),
                    new NewTopic(prefix + "snapshot", 1, (short) 1),
                    new NewTopic(prefix + "snapshotOrdering", 1, (short) 1)
                    ));
            rc.all().get();
        }
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var result = producer.send(new ProducerRecord<>(prefix + "snapshot", Snapshot.newBuilder()
                .setReplicaId("initializer")
                .setOperationsOffset(-1)
                .setSnapshotOrderingOffset(-1)
                .putAllTable(Map.of())
                .putAllClientCounters(Map.of())
                .build().toByteArray()));
        result.get();
        return 0;

    }
    @Command
    int get(@Parameters(paramLabel = "key") String key,
            @Parameters(paramLabel = "clientId") String id,
            @Parameters(paramLabel = "grpcHost:port") String server) {
        var clientXid = ClientXid.newBuilder().setClientid(id).setCounter((int)(System.currentTimeMillis()/1000)).build();
        var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
        var rsp = stub.get(GetRequest.newBuilder().setKey(key).setXid(clientXid).build());
        System.out.println(rsp.getValue());
        return 0;
    }

        @Command
    int inc(@Parameters(paramLabel = "key") String key,
            @Parameters(paramLabel = "amount") int amount,
            @Parameters(paramLabel = "clientId") String id,
            @Option(names = "--repeat") boolean repeat,
            @Option(names = "--concurrent") boolean concurrent,
            @Parameters(paramLabel = "grpclear" +
                    "cHost:port", arity = "1..*") String[] servers) {
        int count = repeat ? 2 : 1;
        var clientXid = ClientXid.newBuilder().setClientid(id).setCounter((int)(System.currentTimeMillis()/1000)).build();
        System.out.println(clientXid);
        for (int i = 0; i < count; i++) {
            var s = Arrays.stream(servers);
            if (concurrent) s = s.parallel();
            var result = s.map(server -> {
                var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
                try {
                    stub.inc(IncRequest.newBuilder().setKey(key).setIncValue(amount).setXid(clientXid).build());
                    return server + ": success";
                } catch (Exception e) {
                    return server + ": " + e.getMessage();
                }
            }).collect(Collectors.joining(", "));
            System.out.println(result);
        }
        return 0;
    }

    @Command
    int replica(@Parameters(paramLabel = "kafkaHost:port") String server,
                @Parameters(paramLabel = "name") String name,
                @Parameters(paramLabel = "port") int port,
                @Parameters(paramLabel = "snapshotCycle") int snapshotCycle,
                @Parameters(paramLabel = "topicPrefix") String topicPrefix) throws IOException, InterruptedException {
        new KafkaTableInstance(server, name, port, snapshotCycle, topicPrefix).start();
        return 0;
    }


    @Command
    int basicTest(@Parameters(paramLabel = "clientId") String id,
                  @Parameters(paramLabel = "init index to debug") int serverIndex,
                  @Parameters(paramLabel = "grpclear" + "cHost:port", arity = "1..*") String [] servers) throws InterruptedException {
        String TESTING_KEY = "testing_key";
        int TESTING_INCREMENT = 1;
        int INCREMENTS = 20;
        int currVal = 0;
        int lastClientCounter;

        HashMap<String, ManagedChannel> channelHashMap = getChannelsAndResetValues(servers, serverIndex, id, TESTING_KEY);
        var stub = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]));
        KafkaTableDebugResponse response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
        Snapshot snapshot = response.getSnapshot();
        lastClientCounter = snapshot.getClientCountersOrDefault(id, -1);

        for(String server: servers) {
            System.out.println("INC REQUESTS FOR " + server);
            // do 20 puts to a replica
            for (int i = 0; i < INCREMENTS; i++) {
                KafkaTableGrpc.newBlockingStub(channelHashMap.get(server))
                        .inc(IncRequest.newBuilder()
                                .setKey(TESTING_KEY)
                                .setIncValue(TESTING_INCREMENT)
                                .setXid(ClientXid.newBuilder()
                                        .setClientid(id)
                                        .setCounter(++lastClientCounter)
                                        .build()).build());
                System.out.println(i+1 + "/"+ INCREMENTS + " REQUESTS DONE");
            }
            currVal += 20;
            System.out.println("REQUESTS COMPLETED, VERIFYING GET FOR SERVERS");
            Thread.sleep(1000);

            // do gets to all the replicas to make sure that they have seen the puts

            if (verifyGet(servers, channelHashMap, TESTING_KEY, id, lastClientCounter, currVal) == -1) return -1;

            response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
            snapshot = response.getSnapshot();
            lastClientCounter = snapshot.getClientCountersOrDefault(id, -1);

            Thread.sleep(1000);
            if (verifyingSnapshots(servers, channelHashMap, TESTING_KEY, id) == -1) return -1;
        }
        System.out.println("ALL TESTS PASSED! CHECK SNAPSHOTS FOR SYNC");
        return 0;
    }

    @Command
    int basicDupTest(@Parameters(paramLabel = "clientId") String id,
                     @Parameters(paramLabel = "init index to debug") int serverIndex,
                     @Parameters(paramLabel = "grpclear" + "cHost:port", arity = "1..*") String [] servers) throws InterruptedException {
        String TESTING_KEY = "testing_key";
        int TESTING_INCREMENT = 1;
        int INCREMENTS = 20;
        int lastClientCounter;

        HashMap<String, ManagedChannel> channelHashMap = getChannelsAndResetValues(servers, serverIndex, id, TESTING_KEY);
        var stub = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]));
        KafkaTableDebugResponse response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
        Snapshot snapshot = response.getSnapshot();
        lastClientCounter = snapshot.getClientCountersOrDefault(id, -1);

        // do the same 20 puts to all the replicas (duplicates)
        for (int i = 0; i < INCREMENTS; i++) {
            int counter = ++lastClientCounter;
            CountDownLatch countDownLatch = new CountDownLatch(servers.length);
            for (String server : servers) {
                KafkaTableGrpc.newStub(channelHashMap.get(server))
                        .inc(IncRequest.newBuilder()
                                .setKey(TESTING_KEY)
                                .setIncValue(TESTING_INCREMENT)
                                .setXid(ClientXid.newBuilder()
                                        .setClientid(id)
                                        .setCounter(counter)
                                        .build()).build(), new StreamObserver<IncResponse>() {
                            @Override
                            public void onNext(IncResponse incResponse) {

                            }

                            @Override
                            public void onError(Throwable throwable) {

                            }

                            @Override
                            public void onCompleted() {
                                countDownLatch.countDown();
                            }
                        });
            }
            countDownLatch.await();
            System.out.println(i+1 + "/" + INCREMENTS + " INCREMENTS DONE");
        }

        System.out.println("REQUESTS COMPLETED, VERIFYING GET FOR SERVERS");
        Thread.sleep(1000);

        // do gets to all the replicas to make sure that they applied the puts only applied once

        if (verifyGet(servers, channelHashMap, TESTING_KEY, id, lastClientCounter, 20) == -1) return -1;

        Thread.sleep(1000);
        if (verifyingSnapshots(servers, channelHashMap, TESTING_KEY, id) == -1) return -1;
        System.out.println("ALL TESTS PASSED! CHECK SNAPSHOTS FOR SYNC");
        return 0;
    }

    @Command
    int recoverTests(@Parameters(paramLabel = "clientId") String id,
                     @Parameters(paramLabel = "init index to debug") int serverIndex,
                     @Parameters(paramLabel = "grpclear" + "cHost:port", arity = "1..*") String [] servers) throws InterruptedException {
        String TESTING_KEY = "testing_key";
        int TESTING_INCREMENT = 1;
        AtomicInteger lastClientCounter = new AtomicInteger();
        AtomicBoolean isRunning = new AtomicBoolean(true);

        HashMap<String, ManagedChannel> channelHashMap = getChannelsAndResetValues(servers, serverIndex, id, TESTING_KEY);
        var stub = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]));
        KafkaTableDebugResponse response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
        Snapshot snapshot = response.getSnapshot();
        lastClientCounter.set(snapshot.getClientCountersOrDefault(id, -1));

        AtomicInteger requestCount = new AtomicInteger(0);

        Thread incrementThread = new Thread(() -> {
            System.out.println("STARTED REQUESTS");
            while (isRunning.get()) {
                for(String server: servers) {
                    if (isRunning.get()) {
                        try {
                            KafkaTableGrpc.newBlockingStub(channelHashMap.get(server))
                                    .inc(IncRequest.newBuilder()
                                            .setKey(TESTING_KEY)
                                            .setIncValue(TESTING_INCREMENT)
                                            .setXid(ClientXid.newBuilder()
                                                    .setClientid(id)
                                                    .setCounter(lastClientCounter.incrementAndGet())
                                                    .build()).build());

                            int curr = requestCount.incrementAndGet();
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        incrementThread.start();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            String str = scanner.next();
            if (Objects.equals(str, "exit")) {
                isRunning.set(false);
                Thread.sleep(1000);
                if (verifyGet(servers, channelHashMap, TESTING_KEY, id, lastClientCounter.incrementAndGet(), requestCount.get()) == -1) return -1;
                Thread.sleep(1000);
                verifyingSnapshots(servers, channelHashMap, TESTING_KEY, id);
                break;
            } else if (Objects.equals(str, "status")) {
                System.out.println(requestCount.get() + " REQUESTS DONE");
            } else if (str.matches("172\\.27\\.24\\.[0-9]+:[0-9]+")) {
                try {
                    KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(str))
                            .exit(ExitRequest.newBuilder().build());
                    System.out.println(str + " EXITED");
                } catch (Exception e) {
                    System.out.println("EXIT EXCEPTION " + e);
                }
            }
            Thread.sleep(100);
        }

        return 0;
    }

    @Command
    int snapshotRecoverTest(@Parameters(paramLabel = "kafkaHost:port") String server,
                            @Parameters(paramLabel = "clientId") String id,
                            @Parameters(paramLabel = "prefix") String prefix,
                            @Parameters(paramLabel = "snapshot cycle value") int snapshotCycle,
                            @Parameters(paramLabel = "init index to debug") int serverIndex,
                            @Parameters(paramLabel = "grpclear" + "cHost:port", arity = "1..*") String[] servers) throws InterruptedException, InvalidProtocolBufferException {
        String TESTING_KEY = "testing_key";
        int TESTING_INCREMENT = 1;
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id + "-snapshotOrdering");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        var consumerSnapshotOrdering = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        System.out.println("Starting at " + new Date());
        var sem = new Semaphore(0);
        AtomicReference<TopicPartition> snapshotOrderingPartition = new AtomicReference<>();
        consumerSnapshotOrdering.subscribe(List.of(prefix + "snapshotOrdering"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("Didn't expect the revoke!");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("Partition assigned");
                collection.stream().forEach(t -> {
                    snapshotOrderingPartition.set(t);
                    consumerSnapshotOrdering.seek(t, 0);
                });
                sem.release();
            }
        });
        System.out.println("first poll count: " + consumerSnapshotOrdering.poll(0).count());
        sem.acquire();
        System.out.println("Ready to consume at " + new Date());
        var records = consumerSnapshotOrdering.poll(Duration.ofSeconds(5));
        ArrayList<SnapshotOrdering> snapshotOrderings = new ArrayList<>();
        long lastOffset = -1;
        for(var record: records) {
            SnapshotOrdering snapshotOrdering = SnapshotOrdering.parseFrom(record.value());
            snapshotOrderings.add(snapshotOrdering);
            lastOffset = record.offset();
        }
        System.out.println("STOP AND RESTART REPLICAS! Press any key to continue...");
        Scanner scanner = new Scanner(System.in);
        scanner.next();

        long offsetToStart = lastOffset - servers.length + 1;
        consumerSnapshotOrdering.seek(snapshotOrderingPartition.get(), offsetToStart);
        consumerSnapshotOrdering.poll(Duration.ZERO);

        records = consumerSnapshotOrdering.poll(Duration.ofSeconds(5));
        long i = offsetToStart;
        for(var record: records) {
            SnapshotOrdering snapshotOrdering = SnapshotOrdering.parseFrom(record.value());
            if(!snapshotOrderings.get((int) i).getReplicaId().equals(snapshotOrdering.getReplicaId())) {
                System.out.println("Order does not match at " + i + " index");
                return -1;
            }
            i++;
        }

        System.out.println("SNAPSHOT ORDERING VERIFIED!");
        System.out.println("DOING INC REQUESTS TO MAKE EVERYONE SNAPSHOT!");

        HashMap<String, ManagedChannel> channelHashMap = getChannelsAndResetValues(servers, serverIndex, id, TESTING_KEY);
        var stub = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]));
        KafkaTableDebugResponse response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
        Snapshot snapshot = response.getSnapshot();
        int lastClientCounter = snapshot.getClientCountersOrDefault(id, -1);

        int numberOfRequests = snapshotCycle * servers.length;
        System.out.println("DOING " + numberOfRequests + " REQUESTS");
        while (numberOfRequests > 0) {
            KafkaTableGrpc.newBlockingStub(channelHashMap.get(servers[numberOfRequests%servers.length]))
                    .inc(IncRequest.newBuilder()
                            .setKey(TESTING_KEY)
                            .setIncValue(TESTING_INCREMENT)
                            .setXid(ClientXid.newBuilder()
                                    .setClientid(id)
                                    .setCounter(++lastClientCounter)
                                    .build()).build());
            numberOfRequests--;
            System.out.println(numberOfRequests + " remaining");
        }
        System.out.println("INC REQUESTS DONE! VERIFY THE SNAPSHOT ORDER:");
        System.out.println(snapshotOrderings.subList((int) offsetToStart, (int) (lastOffset + 1)));
        return 0;
    }


    private int verifyGet(String[] servers, HashMap<String, ManagedChannel> channelHashMap, String TESTING_KEY, String id, int lastClientCounter, int expectedValue) {
        for(String server: servers) {
            int value = KafkaTableGrpc.newBlockingStub(channelHashMap.get(server))
                    .get(GetRequest.newBuilder()
                            .setKey(TESTING_KEY)
                            .setXid(ClientXid.newBuilder()
                                    .setClientid(id)
                                    .setCounter(++lastClientCounter).build()).build()).getValue();
            if (value != expectedValue) {
                System.out.println("get for " + TESTING_KEY + " for " + server + " is incorrect");
                System.out.println("returned: " + value + "; expected: " + expectedValue);
                return -1;
            }
        }
        System.out.println("GET VERIFIED FOR ALL SERVERS");
        return 1;
    }
    private int verifyingSnapshots(String[] servers, HashMap<String, ManagedChannel>  channelHashMap, String TESTING_KEY, String id) {
        ArrayList<Snapshot> snapshots = new ArrayList<>();
        for(String debugReqServer: servers) {
            Snapshot currSnapShot = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(debugReqServer))
                    .debug(KafkaTableDebugRequest.newBuilder().build()).getSnapshot();
            snapshots.add(currSnapShot);
        }
        System.out.println("VERIFYING SNAPSHOTS");
        for (int i = 1; i< snapshots.size(); i++) {
            if(snapshots.get(i).getOperationsOffset() != snapshots.get(i - 1).getOperationsOffset()) {
                System.out.println("snapshots operations offsets don't match");
                return -1;
            }
            if(snapshots.get(i).getSnapshotOrderingOffset() != snapshots.get(i - 1).getSnapshotOrderingOffset()) {
                System.out.println("snapshots snapshot-ordering offsets don't match");
                return -1;
            }
            if(!Objects.equals(snapshots.get(i).getTableMap().get(TESTING_KEY), snapshots.get(i - 1).getTableMap().get(TESTING_KEY))) {
                System.out.println("snapshots key-value pair don't match");
                return -1;
            }
            if(!Objects.equals(snapshots.get(i).getClientCountersMap().get(id), snapshots.get(i - 1).getClientCountersMap().get(id))) {
                System.out.println("snapshots client counters don't match");
                return -1;
            }
        }
        System.out.println("SNAPSHOTS VERIFIED");
        return 1;
    }
    private HashMap<String, ManagedChannel> getChannelsAndResetValues(String[] servers, int serverIndex, String id, String TESTING_KEY) throws InterruptedException {
        HashMap<String, ManagedChannel> channelHashMap = new HashMap<>();
        for(String server: servers) {
            var lastColon = server.lastIndexOf(':');
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(server.substring(0, lastColon), Integer.parseInt(server.substring(lastColon+1)))
                    .usePlaintext().build();
            channelHashMap.put(server, channel);
        }

        System.out.println("Getting snapshot values from " + servers[serverIndex]);
        var stub = KafkaTableDebugGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]));
        KafkaTableDebugResponse response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
        Snapshot snapshot = response.getSnapshot();
        int lastClientCounter = snapshot.getClientCountersOrDefault(id, -1);

        if (snapshot.getTableMap().containsKey(TESTING_KEY) && snapshot.getTableMap().get(TESTING_KEY) > 0) {
            System.out.println("RESETTING " + TESTING_KEY);
            KafkaTableGrpc.newBlockingStub(channelHashMap.get(servers[serverIndex]))
                    .inc(IncRequest.newBuilder()
                            .setKey(TESTING_KEY)
                            .setIncValue(-snapshot.getTableMap().get(TESTING_KEY))
                            .setXid(ClientXid.newBuilder()
                                    .setClientid(id)
                                    .setCounter(++lastClientCounter).build()).build());
            Thread.sleep(1000);
            response = stub.debug(KafkaTableDebugRequest.newBuilder().build());
            snapshot = response.getSnapshot();
            if (snapshot.getTableMap().containsKey(TESTING_KEY) && snapshot.getTableMap().get(TESTING_KEY) > 0) {
                System.out.println("RESETTING `"+ TESTING_KEY +"` DID NOT WORK FOR " + servers[serverIndex]);
                return null;
            }
        }
        return channelHashMap;
    }
    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}