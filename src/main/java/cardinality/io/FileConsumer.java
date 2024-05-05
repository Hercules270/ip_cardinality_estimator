package cardinality.io;

import java.util.function.Consumer;

public interface FileConsumer {

    void readAndConsume(Consumer<String> consumer);

}
