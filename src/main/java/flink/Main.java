package flink;

import MicroChallenger.*;
import flink.queries.Query1;
import flink.queries.Query2;
import flink.queries.Query3;
import flink.source.EndBenchSink;
import flink.source.MicroChallengerSource;
import flink.source.SendResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        MicroChallengerClient client = new MicroChallengerClient();
        String benchId = client.createAndStartBench(false, 10000);
        // Crea il Flink StreamEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Aggiungi la sorgente custom
        DataStream<Batch> stream = env.fromSource(
                new MicroChallengerSource(benchId),
                WatermarkStrategy.forMonotonousTimestamps(),
                "MicroChallengerSource"
        ).map(new BatchDeserializer());

        DataStream<BatchWithMask> q1Stream = Query1.apply(stream);

        // Scrittura su file

        // Converti Batch in stringa CSV
        DataStream<String> csvLines = q1Stream.map(batch ->
                String.format("%d,%s,%d,%d", batch.batchId, batch.printId, batch.tileId, batch.saturated)
        );

        // Crea un sink su file
        FileSink<String> sink = FileSink
                .forRowFormat(new Path("/opt/flink/output/query1"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(10))
                                .withInactivityInterval(Duration.ofMinutes(5)).
                                build()
                )
                .build();

        csvLines.sinkTo(sink);

        //Eseguiamo Query 2
        DataStream<TileWithOutliers> q2Outlier = Query2.applyManhattanDistance(q1Stream);


        //Eseguiamo Query 3
        DataStream<TileClusterResult> clustered = Query3.apply(q2Outlier);
        clustered.map(new SendResult(0, benchId)).name("ResultMapper");

        clustered.map(result -> {
            StringBuilder sb = new StringBuilder();
            sb.append("{'batch_id': ").append(result.batch_id)
                    .append(", 'print_id': '").append(result.print_id).append("'")
                    .append(", 'tile_id': ").append(result.tile_id)
                    .append(", 'saturated': ").append(result.saturated)
                    .append(", 'centroids': [");

            for (int i = 0; i < result.centroids.size(); i++) {
                Centroid c = result.centroids.get(i);
                if (i > 0) sb.append(", ");
                sb.append("{'x': np.float64(").append(c.x)
                        .append("), 'y': np.float64(").append(c.y)
                        .append("), 'count': ").append(c.count).append("}");
            }

            sb.append("]}");
            return sb.toString();
        }).print();

        clustered
                .addSink(new EndBenchSink(benchId))
                .setParallelism(1);

        env.execute("Pipeline Query1,2,3");


    }

    //String result = client.endBench(benchId);
    //LOG.info("Final Result: {}", result);

}