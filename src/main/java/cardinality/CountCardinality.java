package cardinality;

import cardinality.concurrency.IpConsumer;
import cardinality.hash.MurmurHashProvider;
import cardinality.hash.SimpleHashCodeProvider;
import cardinality.hll.HyperLogLog;
import cardinality.io.BufferedReaderConsumer;
import cardinality.io.FileChannelConsumer;
import cardinality.io.OneThreadConsumer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static cardinality.utils.Constants.POISON_PILL;

public class CountCardinality {

    private static final int NUMBER_OF_WORKER_THREADS = 15;

    public static void main(String[] args) throws InterruptedException {
        final var countDownLatch = new CountDownLatch(NUMBER_OF_WORKER_THREADS);
        final var fileConsumer = new OneThreadConsumer(getFileName(args));
        final var bufferedFileConsumer = new BufferedReaderConsumer(getFileName(args));
        final var fileChannelReader = new FileChannelConsumer(getFileName(args));
        final var blockingQueue = new LinkedBlockingQueue<String>();
        final var hyperLogLog = new HyperLogLog<String>(12, new SimpleHashCodeProvider<>());

        final var set = new HashSet<String>();
        long startTime = System.currentTimeMillis();
        Thread readThread = new Thread(() -> fileConsumer.readAndConsume(ipAddress -> consumeIPAddress(set, blockingQueue, ipAddress), () -> action(blockingQueue)));
        Thread readThread2 = new Thread(() -> bufferedFileConsumer.readAndConsume(ipAddress -> doNothing(set, blockingQueue, ipAddress), () -> action(blockingQueue)));
        Thread readThread3 = new Thread(() -> fileChannelReader.readAndConsume(ipAddress -> doNothing(set, blockingQueue, ipAddress), () -> action(blockingQueue)));

//        readThread.start();
//        readThread2.start();
        readThread3.start();
        for (int i = 0; i < NUMBER_OF_WORKER_THREADS; i++) {
            new Thread(new IpConsumer(hyperLogLog, blockingQueue, countDownLatch)).start();
        }
        readThread3.join();
//        countDownLatch.await();
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

    private static void consumeIPAddress(HashSet<String> set, LinkedBlockingQueue<String> blockingQueue, String ipAddress) {
        try {
//            System.out.println("Sending: " + ipAddress);
            set.add(ipAddress);
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
