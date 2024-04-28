package cardinality.hash;

public class SimpleHashCodeProvider<T> implements HashCodeProvider<T> {

    @Override
    public int hashCode(T object) {
        return object.hashCode();
    }

}
