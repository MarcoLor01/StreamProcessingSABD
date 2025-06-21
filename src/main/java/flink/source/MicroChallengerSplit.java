package flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class MicroChallengerSplit implements SourceSplit {
    private final String splitId;

    public MicroChallengerSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}

