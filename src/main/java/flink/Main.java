package flink;

import MicroChallenger.Batch;
import MicroChallenger.BatchDeserializer;
import MicroChallenger.BatchWithMask;
import flink.queries.Query1;
import flink.source.MicroChallengerSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        // Crea il Flink StreamEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Aggiungi la sorgente custom
        DataStream<Batch> stream = env.fromSource(
                new MicroChallengerSource(),
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



        env.execute("Query 1 flink");
    }

}