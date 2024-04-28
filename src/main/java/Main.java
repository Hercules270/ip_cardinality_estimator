import cardinality.hash.MessageDigestHashCodeProvider;
import cardinality.hash.SimpleHashCodeProvider;
import cardinality.hll.HyperLogLog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        MessageDigest instance = MessageDigest.getInstance("SHA-256");
        var log = new HyperLogLog<UUID>(10, new MessageDigestHashCodeProvider<>(instance));
        var set = new HashSet<UUID>();
        var random = new Random();
        Stream.generate(UUID::randomUUID)
                .limit(10000)
                .forEach(uuid -> {
                    log.add(uuid);
                    set.add(uuid);
                });
        System.out.println(log.getCardinality());
        System.out.println(set.size());
    }

    public static void main1(String[] args) {
        System.out.println("Hello world!");

        int number = -456346;
        System.out.println(Integer.toBinaryString(number));
        printBinary(number);
        printBinary(number >>> 2);
        printBinary(number >>> (Integer.SIZE - 16));
        System.out.println("======================");
        printBinary(123);
        System.out.println(Integer.toBinaryString(Integer.highestOneBit(123)));
        System.out.println(Integer.numberOfLeadingZeros(123));
    }


    static void convertToByteArray(int number) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.order(ByteOrder.BIG_ENDIAN);
        b.putInt(number);
        System.out.println(Arrays.toString(b.array()));
        for (byte k : b.array()) {
            System.out.println(Integer.toBinaryString(k & 0xFF));
        }
    }
    public static void printBinary(int num) {
        StringBuilder sb = new StringBuilder();
        for (int i = 31; i >= 0; i--) {
            // Append the bit at position i to the StringBuilder
            sb.append((num & (1 << i)) == 0 ? '0' : '1');
        }
        System.out.println(sb.toString());
    }

}