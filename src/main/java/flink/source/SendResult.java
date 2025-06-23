package flink.source;

import MicroChallenger.MicroChallengerClient;
import MicroChallenger.TileClusterResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.msgpack.core.MessagePack;

public class SendResult extends RichMapFunction<TileClusterResult, Void> {
    private final int workerId;
    private final String benchId;
    private transient MicroChallengerClient client;
    public SendResult(int workerId, String benchId){
        this.workerId= workerId;
        this.benchId = benchId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = new MicroChallengerClient();
    }

    @Override
    public Void map(TileClusterResult value) throws Exception {
        client.sendResult(workerId, benchId, value);

        return null;
    }
}

