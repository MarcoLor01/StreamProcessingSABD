package flink.queries;

import MicroChallenger.Batch;
import MicroChallenger.BatchWithMask;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Query1 {
    public static DataStream<BatchWithMask> apply(DataStream<Batch> input) {
        return input.map(batch -> {
            int[][] temp = batch.temp;
            int rows = temp.length;
            int cols = temp[0].length;

            boolean[][] validMask = new boolean[rows][cols];
            int saturatedCount = 0;

            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    int val = temp[i][j];
                    if (val < 5000 || val > 65000) {
                        validMask[i][j] = false;
                        if (val > 65000) saturatedCount++;
                    } else {
                        validMask[i][j] = true;
                    }
                }
            }


            return new BatchWithMask(batch.batchId, batch.printId, batch.tileId, batch.layer, batch.temp, validMask,saturatedCount);
        });
    }
}
