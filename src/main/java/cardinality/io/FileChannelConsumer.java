package cardinality.io;

import cardinality.concurrency.LineReaderTask;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Class divides file in chunks and reads lines in parallel.
 */
public class FileChannelConsumer implements FileConsumer {
    private final String fileName;

    public FileChannelConsumer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void readAndConsume(Consumer<String> consumer) {
        try (FileChannel fileChannel = FileChannel.open(new File(fileName).toPath(), StandardOpenOption.READ);) {
            int numberOfThreads = Runtime.getRuntime().availableProcessors() * 20;
            long chunkSize = fileChannel.size() / numberOfThreads;
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < numberOfThreads; i++) {
                long start = i * chunkSize;
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, chunkSize);
                Thread thread = Thread.startVirtualThread(new LineReaderTask(buffer, consumer));
                threads.add(thread);
            }
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
