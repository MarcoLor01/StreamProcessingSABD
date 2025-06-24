package flink.queries;

import MicroChallenger.BatchWithMask;
import MicroChallenger.Outlier;
import MicroChallenger.PointWithDeviation;
import MicroChallenger.TileWithOutliers;
import flink.Main;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import static flink.utilities.Constants.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.PriorityQueue;


public class Query2 {
    private static final Logger LOG = LoggerFactory.getLogger(Query2.class);
    public static DataStream<TileWithOutliers> applyManhattanDistance(DataStream<BatchWithMask> input) {

        //Partizioniamo lo stream per tileId
        //KeyedStream<BatchWithMask, Integer> keyedStream = input.keyBy(batch -> batch.tileId);
        KeyedStream<BatchWithMask, Integer> keyedStream = input.keyBy(batch -> batch.tileId);
        //Creo finestra count, di dimensione 3 (3 layer) con slide di 1

        return keyedStream.countWindow(WINDOW_LAYERS_NUMBER, SLIDING_BATCH)
                .apply((WindowFunction<BatchWithMask, TileWithOutliers, Integer, GlobalWindow>) (integer, globalWindow, batches, collector) -> {
                    //System.out.println("DEBUG: Esecuzione window function - Key: " + integer);
                    List<BatchWithMask> batchList = new ArrayList<>();
                    batches.forEach(batchList::add);

                    if (batchList.size() == 1) {
                        TileWithOutliers result = new TileWithOutliers(batchList.get(0), new ArrayList<>(), "");
                        collector.collect(result);
                        return;
                    } else if (batchList.size() == 2) {
                        TileWithOutliers result = new TileWithOutliers(batchList.get(1), new ArrayList<>(), "");
                        collector.collect(result);
                        return;
                    }

                    //Identifichiamo i 3 tile
                    BatchWithMask tileBelow = batchList.get(0);
                    BatchWithMask middleTile = batchList.get(1);
                    BatchWithMask tileAbove = batchList.get(2);

                    //OK

                    //Eseguiamo il calcolo
                    //Usiamo per la classifica una priority queue
                    PriorityQueue<PointWithDeviation> top5DeviationPoints =  new PriorityQueue<>(5, Comparator.comparingDouble((PointWithDeviation point) -> point.deviation));
                    List<Outlier> outliers = findOutliers(middleTile, tileBelow, tileAbove, top5DeviationPoints);

                    String top5Deviation = formatTop5Deviation(top5DeviationPoints, tileAbove.batchId, tileAbove.printId, tileAbove.tileId);

                    TileWithOutliers result = new TileWithOutliers(tileAbove, outliers, top5Deviation);
                    collector.collect(result);
                }).returns(TileWithOutliers.class);
    }

    private static List<Outlier> findOutliers(BatchWithMask middleTile, BatchWithMask tileBelow, BatchWithMask tileAbove, PriorityQueue<PointWithDeviation> top5DeviationPoint) {
        List<Outlier> outliers = new ArrayList<>();

        int rows = tileAbove.temp.length; //Numero di righe
        int cols = tileBelow.temp[0].length; //Numero di colonne

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                if (!tileAbove.validMask[i][j]) {
                    continue;
                }
                List<Integer> nearestNeighborTemps  = new ArrayList<>();
                List<Integer> externalNeighborTemps  = new ArrayList<>();


                for (int distZ = 0; distZ <= 2; distZ++) {
                    //Tile da analizzare ora
                    BatchWithMask actualTile = tileAbove;
                    if (distZ == 1) actualTile = middleTile;
                    if (distZ == 2) actualTile = tileBelow;

                    for (int distX = -4; distX <= 4; distX++) {
                        for (int  distY = -4; distY <= 4; distY++) {
                            //Valore della distanza di Manhattan attuale
                            int manhattanDistance = Math.abs(distX) + Math.abs(distY) + Math.abs(distZ);

                            //Calcolo internal neighbors
                            if (manhattanDistance <= INTERNAL_NEIGHBORS_DISTANCE) {
                                nearestNeighborTemps.add(safeAccess(actualTile, i + distX, j + distY));
                            }

                            //Calcolo external neighbors
                            if (manhattanDistance > INTERNAL_NEIGHBORS_DISTANCE && manhattanDistance <= INTERNAL_NEIGHBORS_DISTANCE * 2){
                                externalNeighborTemps.add(safeAccess(actualTile, i + distX, j + distY));
                            }
                        }
                    }
                }
                double avgNearest = nearestNeighborTemps.stream().mapToInt(Integer::intValue).average().orElse(0.0);
                double avgExternal = externalNeighborTemps.stream().mapToInt(Integer::intValue).average().orElse(0.0);
                double localTemperatureDeviation = Math.abs(avgNearest - avgExternal);

                if (localTemperatureDeviation > OUTLIER_THRESHOLD) {
                    outliers.add(new Outlier(i, j, localTemperatureDeviation));
                }
                if (top5DeviationPoint.size() < 5) {
                    top5DeviationPoint.add(new PointWithDeviation(i, j, localTemperatureDeviation)); // Se ho meno di 5 elementi aggiungo sempre
                } else if (localTemperatureDeviation > top5DeviationPoint.peek().deviation) { //Se rientra nei top 5 attuali
                    top5DeviationPoint.poll(); //Tolgo il valore più basso
                    top5DeviationPoint.add(new PointWithDeviation(i, j, localTemperatureDeviation)); //Add del punto
                }
            }
        }

        return outliers;
    }

    private static int safeAccess(BatchWithMask tile, int x, int y) {
        if (tile != null && x >= 0 && y >= 0 && x < tile.temp.length && y < tile.temp[x].length) {
                return tile.temp[x][y];
        } return 0; // Restituisce 0 se fuori dai bordi
    }

    private static String formatTop5Deviation(PriorityQueue<PointWithDeviation> pointsWithDeviation, int batchId, String printId, int tileId) {
        StringBuilder output = new StringBuilder();
        output.append(batchId).append(",").append(printId).append(",").append(tileId);

        List<PointWithDeviation> sortedPoints = new ArrayList<>(pointsWithDeviation);
        sortedPoints.sort(Comparator.comparingDouble((PointWithDeviation point) -> point.deviation).reversed());

        for (PointWithDeviation point : sortedPoints) {
            output.append(",\"(").append(point.x).append(",").append(point.y).append(")\"")
                    .append(",").append(point.deviation);
        }

        return output.toString();
    }

}
