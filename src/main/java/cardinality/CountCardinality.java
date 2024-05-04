package cardinality;

import cardinality.hash.MurmurHashProvider;
import cardinality.hash.SimpleHashCodeProvider;
import cardinality.hll.HyperLogLog;
import cardinality.io.BufferedReaderConsumer;
import cardinality.io.FileChannelConsumer;
import cardinality.io.OneThreadConsumer;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static cardinality.utils.Constants.NUMBER_OF_WORKER_THREADS;
import static cardinality.utils.Constants.POISON_PILL;

public class CountCardinality {


    public static void main(String[] args) throws InterruptedException {
        final var countDownLatch = new CountDownLatch(NUMBER_OF_WORKER_THREADS);
        final var fileConsumer = new OneThreadConsumer(getFileName(args));
        final var bufferedFileConsumer = new BufferedReaderConsumer(getFileName(args));
        final var fileChannelReader = new FileChannelConsumer(getFileName(args));
        final var blockingQueue = new LinkedBlockingQueue<String>();
        final var hyperLogLog = new HyperLogLog<String>(12, new MurmurHashProvider<>());

        final var set = new HashSet<String>(80_370_000 * 2);
        long startTime = System.currentTimeMillis();
        Thread readThread = new Thread(() -> fileConsumer.readAndConsume(ipAddress -> addIpAddressToQueue(set, blockingQueue, ipAddress), () -> action(blockingQueue)));
        Thread readThread2 = new Thread(() -> bufferedFileConsumer.readAndConsume(ipAddress -> doNothing(set, blockingQueue, ipAddress), () -> action(blockingQueue)));
        Thread readThread3 = new Thread(() -> fileChannelReader.readAndConsume(ipAddress -> addIpAddressToHLL(ipAddress, set, hyperLogLog), () -> action(blockingQueue)));
        System.out.println("====================");
        System.out.println("Start time is " + LocalDateTime.now());

//        readThread.start();
//        readThread2.start();
        readThread3.start();
//        new Thread(new IpConsumer(hyperLogLog, blockingQueue, countDownLatch)).start();
//        for (int i = 0; i < 1; i++) {
//            new Thread(new IpConsumer(hyperLogLog, blockingQueue, countDownLatch)).start();
//        }
        readThread3.join();
        System.out.println("====================");
        System.out.println("Joined producer threads " + LocalDateTime.now());
//        for (int i = 0; i < 19; i++) {
//            new Thread(new IpConsumer(hyperLogLog, blockingQueue, countDownLatch)).start();
//        }
//        countDownLatch.await();
        System.out.println("===================");
        System.out.println("Joined consumer threads " + LocalDateTime.now());
        long cardinality = hyperLogLog.getCardinality();
        long endTime = System.currentTimeMillis();
        System.out.println("Cardinality of IP addresses is: " + cardinality);
        System.out.println("Real size is: " + set.size());
        System.out.println("Ratio is " + (double) (cardinality - set.size()) / set.size() * 100.0 + "%");
        System.out.println("Time take is: " + (endTime - startTime));
    }

    private static void addElementInHyperLogLog(HyperLogLog<String> hyperLogLog, String ip) {
        hyperLogLog.add(ip);
    }

    private static void doNothing(HashSet<String> set, LinkedBlockingQueue<String> blockingQueue, String ipAddress) {
    }

    private static void action(LinkedBlockingQueue<String> blockingQueue) {
        try {
            for (int i = 0; i < NUMBER_OF_WORKER_THREADS; i++) {
                System.out.println("Sending Poison pill " + POISON_PILL);
                blockingQueue.put(POISON_PILL);
            }
//                        blockingQueue.notifyAll();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private static void saveInSet(Set<String> set, String ipAddress) {
        set.add(ipAddress);
    }

    private static void addIpAddressToHLL(String ipAddress, HashSet<String> set, HyperLogLog<String> hll) {
//        if(set.size() % 10000 == 0) {
//            System.out.println("Adding of size " + set.size());
//        }
//        set.add(ipAddress);
        hll.add(ipAddress);
    }
    static int sizeLimit = 20000;

    private static void addIpAddressToQueue(HashSet<String> set, LinkedBlockingQueue<String> blockingQueue, String ipAddress) {
        try {
//            System.out.println("Sending: " + ipAddress);
//            set.add(ipAddress);
            int size = blockingQueue.size();
            if (size > sizeLimit) {
                System.out.println("Current size is " + size);
            }
            if (blockingQueue.size() == Integer.MAX_VALUE) {
                System.out.println("Filled with size: " + blockingQueue.size());
            }
            blockingQueue.put(ipAddress);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private static String getFileName(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("IP address path should be provided");
        }
        return args[0];
    }
    // concurrency
}
