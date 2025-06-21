package flink.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.nio.charset.StandardCharsets;


public class MicroChallengerSource implements Source<byte[], MicroChallengerSplit, Void> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }



    @Override
    public SourceReader<byte[], MicroChallengerSplit> createReader(SourceReaderContext context) {
        return new MicroChallengerReader(context);
    }

    @Override
    public SplitEnumerator<MicroChallengerSplit, Void> createEnumerator(SplitEnumeratorContext<MicroChallengerSplit> context) {
        return new MicroChallengerEnumerator(context);
    }

    @Override
    public SplitEnumerator<MicroChallengerSplit, Void> restoreEnumerator(SplitEnumeratorContext<MicroChallengerSplit> context, Void checkpoint) {
        return createEnumerator(context);
    }

    @Override
    public SimpleVersionedSerializer<MicroChallengerSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() { return 1; }

            @Override
            public byte[] serialize(MicroChallengerSplit split) {
                return split.splitId().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public MicroChallengerSplit deserialize(int version, byte[] serialized) {
                return new MicroChallengerSplit(new String(serialized, StandardCharsets.UTF_8));
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override public int getVersion() { return 1; }
            @Override public byte[] serialize(Void state) { return new byte[0]; }
            @Override public Void deserialize(int version, byte[] serialized) { return null; }
        };
    }
}

