package cardinality.model;

public class IPAddress {

    private final String value;

    private IPAddress(String value){
        this.value = value;
    }
    
    public static IPAddress of(String value) {
        return new IPAddress(value);
    }
}
