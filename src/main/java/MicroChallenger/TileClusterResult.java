package MicroChallenger;

import java.util.List;

public class TileClusterResult {
    public int batch_id;
    public String print_id;
    public int tile_id;
    public int saturated;
    public List<Centroid> centroids;

    public TileClusterResult(int batch_id, String print_id, int tile_id, int saturated, List<Centroid> centroids) {
        this.batch_id = batch_id;
        this.print_id = print_id;
        this.tile_id = tile_id;
        this.saturated = saturated;
        this.centroids = centroids;
    }
}
