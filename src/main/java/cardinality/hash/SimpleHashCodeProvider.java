package cardinality.hash;

public class SimpleHashCodeProvider<T> implements HashCodeProvider<T> {

    @Override
    public long hashCode(T object) {
        return object.hashCode();
    }

}
