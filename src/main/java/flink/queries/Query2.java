package flink.queries;

import MicroChallenger.BatchWithMask;
import MicroChallenger.Outlier;
import MicroChallenger.PointWithDeviation;
import MicroChallenger.TileWithOutliers;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import static flink.utilities.Constants.*;

import java.io.FileWriter;
import java.io.IOException;


public class Query2 {

    public static DataStream<TileWithOutliers> applyManhattanDistance(DataStream<BatchWithMask> input) {
        //Partizioniamo lo stream per tileId
        //KeyedStream<BatchWithMask, Integer> keyedStream = input.keyBy(batch -> batch.tileId);
        KeyedStream<BatchWithMask, Integer> keyedStream = input.keyBy(batch -> {
            return batch.tileId;
        });
        //Creo finestra count, di dimensione 3 (3 layer) con slide di 1

        return keyedStream.countWindow(WINDOW_LAYERS_NUMBER, SLIDING_BATCH)
                .apply((WindowFunction<BatchWithMask, TileWithOutliers, Integer, GlobalWindow>) (integer, globalWindow, batches, collector) -> {
                    System.out.println("DEBUG: Esecuzione window function - Key: " + integer);
                    List<BatchWithMask> batchList = new ArrayList<>();
                    batches.forEach(batchList::add);

                    if (batchList.size() < 3) {
                        // Non dovrebbe servire, solo per robustezza
                        return;
                    }

                    //Identifichiamo i 3 tile
                    BatchWithMask tileBelow = batchList.get(0);
                    BatchWithMask middleTile = batchList.get(1);
                    BatchWithMask tileAbove = batchList.get(2);

                    //OK

                    //Eseguiamo il calcolo
                    List<PointWithDeviation> pointWithDeviations = new ArrayList<>(); //Per la classifica
                    List<Outlier> outliers = findOutliers(middleTile, tileBelow, tileAbove, pointWithDeviations);

                    /*
                    System.out.println("DEBUG - Lista degli outlier per il tileId: " + tileAbove.tileId + "del layer: " + tileAbove.layer + "\n");
                    System.out.println("Numero di outlier: %d\n" + outliers.size());
                    outliers.sort(Comparator.comparingInt(o -> o.y));
                    for(Outlier outlier : outliers) {
                        System.out.println("Coordinate: " + outlier.x + ", " + outlier.y + "valore deviazione: " + outlier.deviation);
                    } */

                    printTop5Deviation(pointWithDeviations, tileAbove.batchId, tileAbove.printId, tileAbove.tileId);

                    TileWithOutliers result = new TileWithOutliers(tileAbove, outliers);
                    collector.collect(result);
                }).returns(TileWithOutliers.class);
    }

    private static List<Outlier> findOutliers(BatchWithMask middleTile, BatchWithMask tileBelow, BatchWithMask tileAbove, List<PointWithDeviation> pointWithDeviations) {
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

                pointWithDeviations.add(new PointWithDeviation(i , j ,localTemperatureDeviation)); //Aggiungo il punto alla lista

            }
        }

        return outliers;
    }

    private static int safeAccess(BatchWithMask tile, int x, int y) {
        if (tile != null && x >= 0 && y >= 0 && x < tile.temp.length && y < tile.temp[x].length) {
                return tile.temp[x][y];
        } return 0; // Restituisce 0 se fuori dai bordi
    }

    private static void printTop5Deviation(List<PointWithDeviation> pointsWithDeviation, int batchId, String printId, int tileId) {
        pointsWithDeviation.sort((a, b) -> Double.compare(b.deviation, a.deviation));

        // Prendi i primi 5 (o meno se non ci sono abbastanza punti)
        List<PointWithDeviation> top5Deviation = pointsWithDeviation.subList(0, Math.min(5, pointsWithDeviation.size()));

        StringBuilder output = new StringBuilder();
        output.append(batchId).append(",").append(printId).append(",").append(tileId);

        for (PointWithDeviation point : top5Deviation) {
            output.append(",\"(").append(point.x).append(",").append(point.y).append(")\"")
                    .append(",").append(point.deviation);
        }

        try (FileWriter writer = new FileWriter("/opt/flink/output/query2.csv", true)) { // true per appendere
            writer.write(output.toString());
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
