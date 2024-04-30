package cardinality.hll;

import cardinality.hash.HashCodeProvider;
import cardinality.hash.SimpleHashCodeProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class HyperLogLog<T> implements CardinalityEstimator<T> {

    private static final int DEFAULT_LOG_OF_NUMBER_OF_REGISTERS = 5;
    private static final int MINIMUM_LOG_OF_NUMBER_OF_REGISTERS = 2;
    private static final int MAXIMUM_LOG_OF_NUMBER_OF_REGISTERS = 16;
    private static final HashCodeProvider DEFAULT_HASH_CODE_PROVIDER = new SimpleHashCodeProvider<>();
    private static final double SMALL_RANGE_CORRECTION_COEFFICIENT = 5 / 2;
    private static final double LARGE_RANGE_CORRECTION_COEFFICIENT = (1.0 / 30) * Math.pow(2, 32);

    private final int logOfNumberOfRegisters; // b where m = 2^b
    private final int numberOfRegisters; // m
    private final AtomicLong[] registers; // M[j] registers
    private final HashCodeProvider<T> hashCodeProvider; // h(D) hash function
    private final int registerIndexMask;
    private final Map<String, Integer> map;

    public HyperLogLog() {
        this(DEFAULT_LOG_OF_NUMBER_OF_REGISTERS, DEFAULT_HASH_CODE_PROVIDER);
    }

    public HyperLogLog(int logOfNumberOfRegisters, HashCodeProvider<T> hashCodeProvider) {
        validateNumberOfRegisters(logOfNumberOfRegisters);

        this.logOfNumberOfRegisters = logOfNumberOfRegisters;
        this.numberOfRegisters = 1 << logOfNumberOfRegisters;
        this.registerIndexMask = numberOfRegisters - 1;
        this.registers = initializeRegisters(numberOfRegisters);
        this.hashCodeProvider = hashCodeProvider;
        this.map = new HashMap<>();

    }

    private AtomicLong[] initializeRegisters(int numberOfRegisters) {
        var array = new AtomicLong[numberOfRegisters];
        for (int i = 0; i < array.length; i++) {
            array[i] = new AtomicLong(0);
        }
        return array;
    }

    private void validateNumberOfRegisters(int logOfNumberOfRegisters) {
        if (logOfNumberOfRegisters < MINIMUM_LOG_OF_NUMBER_OF_REGISTERS && logOfNumberOfRegisters > MAXIMUM_LOG_OF_NUMBER_OF_REGISTERS) {
            throw new IllegalArgumentException("Logarithm for number of registers must be between " + MINIMUM_LOG_OF_NUMBER_OF_REGISTERS + " and "
                    + MAXIMUM_LOG_OF_NUMBER_OF_REGISTERS +
                    " (value was " + logOfNumberOfRegisters + ").");
        }
    }

    @Override
    public void add(T t) {
        long hashCode = hashCodeProvider.hashCode(t); // step 1 get hashcode
        int registerIndex = getRegisterNumber(hashCode);
        int positionOfMostSignificantBit = getPositionOfMostSignificantBit(hashCode);
//        if (positionOfMostSignificantBit > 25) {
//            System.out.println(Thread.currentThread().getName() + "Whatafuck is going on");
//        }
        registers[registerIndex].set(Math.max(positionOfMostSignificantBit, registers[registerIndex].get()));
    }

    private int getPositionOfMostSignificantBit(long hashCode) {
        return Long.numberOfLeadingZeros(hashCode) + 1;
    }

    private int getRegisterNumber(long hashCode) {
        return (int) hashCode & registerIndexMask;
    }

    @Override
    public long getCardinality() {
        final double harmonicMeanOfRegisters = getHarmonicMeanOfRegisters();
        System.out.println("Harmonic mean of registers is " + harmonicMeanOfRegisters);
        final double correctionCoefficient = calculateAlpha(numberOfRegisters);
        System.out.println("Correction coefficient is " + correctionCoefficient);
        final double estimate = correctionCoefficient * Math.pow(numberOfRegisters, 2) * harmonicMeanOfRegisters;
        System.out.println("Initial estimate is " + estimate);
        long adjustEstimate = adjustEstimate(estimate);
        System.out.println("Adjusted estimate is " + adjustEstimate);
        printRegisters();
        return adjustEstimate;
    }

    private void printRegisters() {

        Map<Long, Long> map = new HashMap<>();
        for (AtomicLong x : registers) {
            if (map.get(x.get()) == null) {
                map.put(x.get(), 0L);
            }
            map.put(x.get(), map.get(x.get()) + 1);
        }
        System.out.println("Registers is " + map);
    }

    private long adjustEstimate(double estimate) {
        if (requiresSmallRangeCorrection(estimate)) {
            long x = adjustSmallRange(estimate);
            System.out.println("Small range adjustment is " + x);
            return x;
        }
        return Double.valueOf(estimate).longValue();
    }

    private long adjustLargeRange(double estimate) {
        return Double.valueOf(Math.pow(2, 32) * Math.log(1 - estimate / Math.pow(2, 32))).longValue();
    }

    private long adjustSmallRange(double estimate) {
        long numberOfZeroRegisters = Arrays.stream(registers)
                .filter(atomicLong -> atomicLong.longValue() == 0)
                .count();
        if (numberOfZeroRegisters != 0) {
            return Double.valueOf(numberOfRegisters * Math.log(numberOfRegisters / numberOfZeroRegisters)).longValue();
        }
        return Double.valueOf(estimate).longValue();
    }

    private boolean requiresLargeRangeCorrection(double estimate) {
        return estimate > LARGE_RANGE_CORRECTION_COEFFICIENT;
    }

    private boolean requiresSmallRangeCorrection(double estimate) {
        return estimate < SMALL_RANGE_CORRECTION_COEFFICIENT;
    }

    private double getHarmonicMeanOfRegisters() {
        double harmonicMeanOfRegisters = 0;
        for (AtomicLong value : registers) {
            harmonicMeanOfRegisters += 1.0 / (1 << value.get());
        }
        return 1.0 / harmonicMeanOfRegisters;
    }

    private static double calculateAlpha(int numberOfRegisters) {
        return switch (numberOfRegisters) {
            case 4, 8, 16 -> 0.673;
            case 32 -> 0.697;
            case 64 -> 0.709;
            default -> 0.7213 / (1 + 1.079 / numberOfRegisters);
        };
    }
}
