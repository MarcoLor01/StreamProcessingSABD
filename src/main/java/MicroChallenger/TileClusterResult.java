package MicroChallenger;

public class TileClusterResult extends TileWithOutliers{
    public final double[][] centroids;
    public final int[] clusterSizes;

    public TileClusterResult(TileWithOutliers tileWithOutliers, double[][] centroids, int[] clusterSizes) {
        super(tileWithOutliers);
        this.centroids = centroids;
        this.clusterSizes = clusterSizes;
    }


}
