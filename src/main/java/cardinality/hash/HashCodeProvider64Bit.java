package cardinality.hash;

public class HashCodeProvider64Bit<T> implements HashCodeProvider<T> {

    private static final long PRIME_NUMBER = 1125899906842597L; // prime number for better distribution
    @Override
    public long hashCode(T object) {
        String str = object.toString();
        int len = str.length();
        long hash = PRIME_NUMBER;
        for (int i = 0; i < len; i++) {
            hash = 31L * hash + str.charAt(i); // mix bits using multiplication with a prime
        }
        return hash;
    }
}
