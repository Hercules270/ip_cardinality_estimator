package cardinality.hash;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class MessageDigestHashCodeProvider<T> implements HashCodeProvider<T> {

    private final MessageDigest messageDigest;

    public MessageDigestHashCodeProvider(MessageDigest messageDigest) {
        this.messageDigest = messageDigest;
    }

    @Override
    public int hashCode(T object) {
        byte[] digest = messageDigest.digest(object.toString().getBytes());
        ByteBuffer bb = ByteBuffer.wrap(digest);
        long aLong = bb.getLong();
        return (int) aLong;
    }

    public static void printBinary(long num) {
        StringBuilder sb = new StringBuilder();
        for (int i = 63; i >= 0; i--) {
            // Append the bit at position i to the StringBuilder
            sb.append((num & (1 << i)) == 0 ? '0' : '1');
        }
        System.out.println(sb.toString());
    }
}
