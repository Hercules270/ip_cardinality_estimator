package cardinality.io;

import cardinality.concurrency.LineReaderThread;
import cardinality.utils.Actionable;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FileChannelConsumer implements FileConsumer {
    private final String fileName;

    public FileChannelConsumer(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void readAndConsume(Consumer<String> consumer, Actionable action) {
        try (FileChannel fileChannel = FileChannel.open(new File(fileName).toPath(), StandardOpenOption.READ);) {
            int numberOfThreads = Runtime.getRuntime().availableProcessors() * 100;
            long chunkSize = fileChannel.size() / numberOfThreads;
            List<LineReaderThread> threads = new ArrayList<>();
            for (int i = 0; i < numberOfThreads; i++) {
                long start = i * chunkSize;
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, chunkSize);
                LineReaderThread lineReaderThread = new LineReaderThread(buffer, consumer);
                threads.add(lineReaderThread);
                lineReaderThread.start();
            }
            long result = 0;
            for (LineReaderThread thread : threads) {
                thread.join();
                result += thread.count();
            }
            System.out.println("REsult is " + result);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            action.doAction();
        }
    }
}
