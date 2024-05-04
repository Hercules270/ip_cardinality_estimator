import cardinality.hash.HashCodeProvider64Bit;
import cardinality.hash.MurmurHashProvider;
import cardinality.hll.HyperLogLog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void cinvertIPToString(String[] args) {
        String s = "14.33.213.57";
        String[] split = s.split("\\.");
        int x = 0;
        for (String val : split) {
            x <<= 8;
            x |= Integer.parseInt(val);
        }
    }


    public static void main(String[] args) throws NoSuchAlgorithmException {
        long d = runAtomicSimulation();
    }

    static long runAtomicSimulation() throws NoSuchAlgorithmException {
        System.out.println("=========================");
        System.out.println("=========================");
        System.out.println("=========================");
        System.out.println("Running atomic simulation");
        System.out.println("=========================");
        System.out.println("=========================");
        System.out.println("=========================");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1; i++) {
            try {
                runningSimulation();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        long difference = System.currentTimeMillis() - startTime;
        System.out.println("Time for atomic simulation: " + difference);
        return difference;
    }

    private static void runningSimulation() throws NoSuchAlgorithmException, IOException {
        System.out.println("------- Starting simulation -------");
        var log = new HyperLogLog<String >(12, new MurmurHashProvider<>());
        int size = 10_000_000;
        Set<String> set = new HashSet<>();
        Files.lines(new File("C:\\zev\\ip_cardinality_estimator\\src\\main\\resources\\ip_addresses").toPath())
//        Stream.generate(UUID::randomUUID)
                .limit(size)
                .forEach(uuid -> {
//                uuid += UUID.randomUUID().toString();
                    log.add(uuid);
                    set.add(uuid);
                });
        long cardinality = log.getCardinality();
        long realSize = set.size();
        System.out.println("Cardinality calculated was: " + cardinality + " vs size: " + realSize);
        double percentage = ((double) (cardinality - realSize)) / realSize * 100;
        System.out.println("Approximation error is " + percentage + "%");
        System.out.println();
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