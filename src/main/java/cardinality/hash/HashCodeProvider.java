package cardinality.hash;

import java.security.NoSuchAlgorithmException;

public interface HashCodeProvider<T> {

    long hashCode(T object);

}
