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
    private final String benchId;
    private boolean finished = false;

    public MicroChallengerReader(SourceReaderContext context,String benchId) {
        this.context = context;
        this.benchId = benchId;
    }

    @Override
    public void start() {
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
