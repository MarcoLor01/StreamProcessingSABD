package flink.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.util.List;

public class MicroChallengerEnumerator implements SplitEnumerator<MicroChallengerSplit, Void> {
    private final SplitEnumeratorContext<MicroChallengerSplit> context;
    private boolean assigned = false;

    public MicroChallengerEnumerator(SplitEnumeratorContext<MicroChallengerSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
        // No background work needed
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!assigned) {
            MicroChallengerSplit split = new MicroChallengerSplit("default-split");
            context.assignSplit(split, subtaskId);
            context.signalNoMoreSplits(subtaskId);
            assigned = true;
        }
    }

    @Override
    public void addSplitsBack(List<MicroChallengerSplit> list, int i) {

    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Void snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public void close() {}
}
