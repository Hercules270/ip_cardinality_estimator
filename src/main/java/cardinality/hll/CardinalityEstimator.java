package cardinality.hll;


public interface CardinalityEstimator<T> {

    void add(T t);

    long getCardinality();

}
