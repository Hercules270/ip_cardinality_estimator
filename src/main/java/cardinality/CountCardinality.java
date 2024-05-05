package cardinality;

import cardinality.hll.HyperLogLog;
import cardinality.io.FileChannelConsumer;
import cardinality.hash.MurmurHashProvider;

/**
 * Entry point of program.
 * Application reads IP addresses from file passed as an argument with an absolute path
 * and calculates approximate cardinality of the IPs.
 */
public class CountCardinality {

    public static void main(String[] args) {
        final var fileChannelReader = new FileChannelConsumer(getFileName(args));
        System.out.println("------> Starting to calculate cardinality of file " + getFileName(args) + " <------");
        final var hyperLogLog = new HyperLogLog<String>(12, new MurmurHashProvider<>());
        fileChannelReader.readAndConsume(ipAddress -> hyperLogLog.add(ipAddress));
        long cardinality = hyperLogLog.getCardinality();
        System.out.println("------> Cardinality of IP addresses: " + cardinality + " <------");
    }

    private static String getFileName(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("IP address path should be provided");
        }
        return args[0];
    }
}
