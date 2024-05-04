package cardinality.io;

import java.io.IOException;

public class TestingRead {

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        String fileName = "C:\\zev\\ip_cardinality_estimator\\src\\main\\resources\\ip_addresses";
        FileConsumer reader = new OneThreadConsumer(fileName);
        int[] k = new int[1];
        reader.readAndConsume(s -> {}, null);
        System.out.println("Lines read is " + k[0]);
//        long diff = Duration.of(System.currentTimeMillis() - startTime, ChronoUnit.MILLIS).toSeconds();
//        System.out.println("Time took seconds: " + diff);
    }
}
