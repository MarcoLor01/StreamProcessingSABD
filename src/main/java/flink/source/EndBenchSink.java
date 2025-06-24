package flink.source;

import MicroChallenger.MicroChallengerClient;
import MicroChallenger.TileClusterResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class EndBenchSink extends RichSinkFunction<TileClusterResult> {

    private static final Logger LOG = LoggerFactory.getLogger(EndBenchSink.class);
    private final String benchId;
    private transient MicroChallengerClient client;

    public EndBenchSink(String benchId) {
        this.benchId = Objects.requireNonNull(benchId);
    }

    @Override
    public void open(Configuration parameters) {
        this.client = new MicroChallengerClient();
    }

    @Override
    public void invoke(TileClusterResult value, Context context) {
        // Not used
    }

    @Override
    public void close() throws Exception {
        String finalResponse = client.endBench(benchId);
        LOG.info("Final Result: {}", finalResponse);
    }

}
