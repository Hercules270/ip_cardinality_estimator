package cardinality.io;

import cardinality.utils.Actionable;

import java.io.IOException;
import java.util.function.Consumer;

public interface FileConsumer {

    void readAndConsume(Consumer<String> consumer, Actionable action) ;

}
