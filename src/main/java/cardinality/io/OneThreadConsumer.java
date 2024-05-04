package cardinality.io;

import cardinality.utils.Actionable;

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
    public void readAndConsume(Consumer<String> consumer, Actionable action) {
        File file = new File(fileName);
        try (Stream<String> lineStream = Files.lines(file.toPath())) {
            lineStream
//                    .skip(1_000_000)
//                    .limit(10_000_000)
//                    .forEach(s -> System.out.println(s));
                    .forEach(s -> {
//                        System.out.println("Producing " + s);
                        consumer.accept(s);
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            action.doAction();
        }
    }
}
