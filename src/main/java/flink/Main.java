package flink;

import MicroChallenger.*;
import flink.queries.Query1;
import flink.queries.Query2;
import flink.queries.Query3;
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

        //Eseguiamo Query 2
        DataStream<TileWithOutliers> q2Outlier = Query2.applyManhattanDistance(q1Stream);

        // 1) Stream di chi ha outlier
        DataStream<TileWithOutliers> haveOutliers =
                q2Outlier
                        .filter(t -> !t.outliers.isEmpty());

        // 2) Clusterizzo solo quelli
        DataStream<TileClusterResult> clustered =
                Query3.apply(haveOutliers);

        // 3) Risultati vuoti per chi non ha outlier
        DataStream<TileClusterResult> emptyResults =
                q2Outlier
                        .filter(t -> t.outliers.isEmpty())
                        .map(t -> new TileClusterResult(
                                t,
                                new double[0][],   // nessun centroide
                                new int[0]         // nessuna size
                        ));

        // 4) Unisco tutto
        DataStream<TileClusterResult> allResults =
                clustered.union(emptyResults);

        //TODO: chiamata a container per risultati finali
        env.execute("Pipeline Query1,2,3");
    }

}