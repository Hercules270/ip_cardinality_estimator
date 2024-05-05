package cardinality.hll;

import cardinality.hash.HashCodeProvider;
import cardinality.hash.SimpleHashCodeProvider;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Implementation of HyperLogLog data structure, which approximates cardinality of elements in multiset.
 * Standard error of the result is ±2%.
 */
public class HyperLogLog<T> implements CardinalityEstimator<T> {

    private static final int DEFAULT_LOG_OF_NUMBER_OF_REGISTERS = 12;
    private static final int MINIMUM_LOG_OF_NUMBER_OF_REGISTERS = 2;
    private static final int MAXIMUM_LOG_OF_NUMBER_OF_REGISTERS = 16;
    private static final HashCodeProvider DEFAULT_HASH_CODE_PROVIDER = new SimpleHashCodeProvider<>();
    private static final double SMALL_RANGE_CORRECTION_COEFFICIENT = 5 / 2;
    private final int logOfNumberOfRegisters;
    private final int numberOfRegisters;
    private final AtomicLong[] registers;
    private final HashCodeProvider<T> hashCodeProvider;
    private final int registerIndexMask;

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
            throw new IllegalArgumentException("Logarithm of number of registers must be between " + MINIMUM_LOG_OF_NUMBER_OF_REGISTERS + " and "
                    + MAXIMUM_LOG_OF_NUMBER_OF_REGISTERS +
                    " (value was " + logOfNumberOfRegisters + ").");
        }
    }

    @Override
    public void add(T t) {
        long hashCode = hashCodeProvider.hashCode(t); // step 1 get hashcode
        int registerIndex = getRegisterNumber(hashCode);
        int positionOfMostSignificantBit = getPositionOfMostSignificantBit(hashCode);
        synchronized (registers[registerIndex]) {
            long maximumValue = Math.max(positionOfMostSignificantBit, registers[registerIndex].get());
            registers[registerIndex].set(maximumValue);
        }
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
        final double correctionCoefficient = calculateAlpha(numberOfRegisters);
        final double estimate = correctionCoefficient * Math.pow(numberOfRegisters, 2) * harmonicMeanOfRegisters;
        long adjustEstimate = adjustEstimate(estimate);
        return adjustEstimate;
    }

    private long adjustEstimate(double estimate) {
        if (requiresSmallRangeCorrection(estimate)) {
            long x = adjustSmallRange(estimate);
            System.out.println("Small range adjustment is " + x);
            return x;
        }
        return Double.valueOf(estimate).longValue();
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
