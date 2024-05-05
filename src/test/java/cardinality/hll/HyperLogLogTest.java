package cardinality.hll;

import cardinality.hash.MurmurHashProvider;

import java.util.UUID;
import java.util.stream.Stream;

public class HyperLogLogTest {

        private static CardinalityEstimator<UUID> cardinalityEstimator = new HyperLogLog<>(12, new MurmurHashProvider<>());

    public static void main(String[] args) {
        int size = 100000;
        while(size <= 10_000_000) {
            testNRecords(size);
            size *= 10;
        }
    }

    private static void testNRecords(int n) {
        System.out.println("========================================");
        System.out.println("Starting test for " + n + " cardinality!");
        Stream.generate(UUID::randomUUID)
                .limit(n)
                .forEach(cardinalityEstimator::add);
        assertInRange(n);
        clear();
    }

    private static void clear() {
        cardinalityEstimator = new HyperLogLog<>();
    }

    private static void assertInRange(int expectedCardinality) {
        long cardinality = cardinalityEstimator.getCardinality();
        if(cardinality < expectedCardinality * 0.97 || cardinality > expectedCardinality * 1.03) {
            throw new RuntimeException("Error during testing. Expected result " + expectedCardinality + " actual result " + cardinality);
        } else {
            System.out.println("TEST PASSED for " + expectedCardinality + "!");
        }
    }
}
