package flink.queries;

import MicroChallenger.Centroid;
import MicroChallenger.Outlier;
import MicroChallenger.TileClusterResult;
import MicroChallenger.TileWithOutliers;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import smile.clustering.DBSCAN;

import java.util.ArrayList;
import java.util.List;

import static flink.utilities.Constants.DBSCAN_MIN_PTS;
import static flink.utilities.Constants.DBSCAN_RADIUS;

public class Query3 {
    public static DataStream<TileClusterResult> apply(DataStream<TileWithOutliers> input) {
        return input
                .map(new ClusterMapFunction())
                .returns(TileClusterResult.class);
    }

    private static class ClusterMapFunction
            implements MapFunction<TileWithOutliers, TileClusterResult> {

        @Override
        public TileClusterResult map(TileWithOutliers tileWithOutliers) {
            List<Outlier> outliers = tileWithOutliers.outliers;

            if (outliers.isEmpty()) {
                return new TileClusterResult(tileWithOutliers.batchId, tileWithOutliers.printId, tileWithOutliers.tileId,
                        tileWithOutliers.saturated, new ArrayList<>());
            }

            //DBSCAN
            double[][] data = outliers.stream()
                    .map(o -> new double[]{o.x, o.y})
                    .toArray(double[][]::new);

            //fit
            DBSCAN<double[]> db = DBSCAN.fit(data, DBSCAN_MIN_PTS, DBSCAN_RADIUS);

            //Etichette dei cluster dei punti
            int[] labels = db.y;
            int numClusters = db.k;

            double[][] centroids = new double[numClusters][data[0].length];
            int[] clusterSizes = new int[numClusters];

            for (int i = 0; i < labels.length; i++) {
                int label = labels[i];
                if (label >= 0 && label < numClusters) {
                    // Accumula per il centroide
                    for (int j = 0; j < data[i].length; j++) {
                        centroids[label][j] += data[i][j];
                    }
                    clusterSizes[label]++;
                }
            }

            List<Centroid> centroidList = new ArrayList<>();
            // Calcolo dei centroidi finali
            for (int c = 0; c < numClusters; c++) {
                if (clusterSizes[c] > 0) {
                    for (int j = 0; j < centroids[c].length; j++) {
                        centroids[c][j] /= clusterSizes[c];
                    }

                    double cx = centroids[c][0];
                    double cy = centroids[c][1];
                    int sz = clusterSizes[c];

                    centroidList.add(new Centroid(cx, cy, sz));
                    /*
                    System.out.printf(
                            "DEBUG - centroid %d: x=%.2f, y=%.2f, size=%d%n",
                            c, cx, cy, sz
                    );

                     */
                }
            }

            return new TileClusterResult(tileWithOutliers.batchId, tileWithOutliers.printId, tileWithOutliers.tileId,
                    tileWithOutliers.saturated, centroidList);
        }
    }
}

