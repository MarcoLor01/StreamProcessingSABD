package flink.source;

import MicroChallenger.MicroChallengerClient;
import MicroChallenger.TileClusterResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SendResult extends RichMapFunction<TileClusterResult, Void> {
    private final int workerId;
    private final String benchId;
    private transient MicroChallengerClient client;
    private static final Logger LOG = LoggerFactory.getLogger(SendResult.class);

    public SendResult(int workerId, String benchId){
        this.workerId= workerId;
        this.benchId = benchId;
    }

    @Override
    public void open(Configuration parameters){
        this.client = new MicroChallengerClient();
    }

    @Override
    public Void map(TileClusterResult value) throws Exception {
        String result = client.sendResult(workerId, benchId, value);
        LOG.info("Result: {}", result);
        return null;
    }
}

