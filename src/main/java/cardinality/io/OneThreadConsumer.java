package cardinality.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class OneThreadConsumer implements FileConsumer {

    private final String fileName;

    public OneThreadConsumer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void readAndConsume(Consumer<String> consumer) {
        File file = new File(fileName);
        try (Stream<String> lineStream = Files.lines(file.toPath())) {
            lineStream.forEach(s -> consumer.accept(s));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
