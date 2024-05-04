package cardinality.io;

import cardinality.utils.Actionable;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class BufferedReaderConsumer implements FileConsumer {

    private final String fileName;

    public BufferedReaderConsumer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void readAndConsume(Consumer<String> consumer, Actionable action)  {
        try {
            final BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {

            }
            in.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

}
