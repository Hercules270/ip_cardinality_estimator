package cardinality.concurrency;

import java.nio.MappedByteBuffer;
import java.util.function.Consumer;


/**
 * Runnable class, that reads chunk of file represented by buffer field.
 * Class applies logic contained in consumer field fore each line of file.
 */
public class LineReaderTask implements Runnable {
    private MappedByteBuffer buffer;
    private Consumer<String> consumer;

    public LineReaderTask(MappedByteBuffer buffer, Consumer<String> consumer) {
        this.buffer = buffer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        StringBuilder builder = new StringBuilder();
        while (buffer.hasRemaining()) {
            char nextChar = (char) buffer.get();
            if (isEndOfLine(nextChar)) {
                consumer.accept(builder.toString());
                builder = new StringBuilder();
            } else {
                builder.append(nextChar);
            }
        }
    }

    private boolean isEndOfLine(char nextChar) {
        return nextChar == '\n';
    }
}
