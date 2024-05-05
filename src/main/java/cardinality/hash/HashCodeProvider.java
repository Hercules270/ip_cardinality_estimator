package cardinality.hash;

public interface HashCodeProvider<T> {
    long hashCode(T object);
}
