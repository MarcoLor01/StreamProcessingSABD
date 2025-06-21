package flink.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import MicroChallenger.MicroChallengerClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MicroChallengerReader implements SourceReader<byte[], MicroChallengerSplit> {
    private final SourceReaderContext context;
    private final MicroChallengerClient client = new MicroChallengerClient();
    private String benchId;
    private boolean running = true;
    private boolean finished = false;

    public MicroChallengerReader(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void start() {
        try {
            benchId = client.createAndStartBench(false, 10000); // VALORE MASSIMO DEI BATCH DA PROCESSARE
        } catch (Exception e) {
            throw new RuntimeException("Failed to start benchmark", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<byte[]> output) throws InterruptedException {
        if (finished) return InputStatus.END_OF_INPUT;

        try {
            byte[] batch = client.nextBatch(benchId);
            if (batch == null) {
                finished = true;
                return InputStatus.END_OF_INPUT;
            } else {
                output.collect(batch);
                return InputStatus.MORE_AVAILABLE;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading batch", e);
        }
    }

    @Override
    public List<MicroChallengerSplit> snapshotState(long checkpointId) {
        return Collections.emptyList(); // non è necessario per ora
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<MicroChallengerSplit> splits) {}

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() {}
}
