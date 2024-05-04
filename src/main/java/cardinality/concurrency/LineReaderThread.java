package cardinality.concurrency;

import java.nio.MappedByteBuffer;
import java.util.function.Consumer;

public class LineReaderThread extends Thread {
    private MappedByteBuffer buffer;
    private Consumer<String> consumer;

    public LineReaderThread(MappedByteBuffer buffer, Consumer<String> consumer) {
        this.buffer = buffer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        StringBuilder builder = new StringBuilder();
        while (buffer.hasRemaining()) {
            char k = (char) buffer.get();
            if (k == '\n') {
                consumer.accept(builder.toString());
                builder = new StringBuilder();
            } else {
                builder.append(k);
            }
        }
    }

    public long count() {
        return 0;
    }

}
