package cardinality.concurrency;

import cardinality.hll.HyperLogLog;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static cardinality.utils.Constants.POISON_PILL;

public class IpConsumer implements Runnable {

    private final HyperLogLog<String> hyperLogLog;
    private final BlockingQueue<String> blockingQueue;
    private final CountDownLatch countDownLatch;

    public IpConsumer(HyperLogLog<String> hyperLogLog, BlockingQueue<String> blockingQueue, CountDownLatch countDownLatch) {
        this.hyperLogLog = hyperLogLog;
        this.blockingQueue = blockingQueue;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {

//        System.out.println("EBEEEEEEE " + Thread.currentThread().getName());
//        countDownLatch.countDown();
        System.out.println("Initializing " + Thread.currentThread().getName());
        while (true) {
            String ipAddress = null;
            try {
                ipAddress = blockingQueue.poll(3, TimeUnit.of(ChronoUnit.SECONDS));
                if(ipAddress == null) {
                    countDownLatch.countDown();
                    return;
//                System.out.println("CONSUMED IP " + ipAddress)
                };
                if (POISON_PILL.equals(ipAddress)) {
                    System.out.println("Found poison pill");
                    countDownLatch.countDown();
                    return;
                }
                hyperLogLog.add(ipAddress);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
