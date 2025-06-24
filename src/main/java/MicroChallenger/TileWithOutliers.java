package MicroChallenger;

import java.util.List;

public class TileWithOutliers extends BatchWithMask {
    public final List<Outlier> outliers;
    public String top5Deviation;

    public TileWithOutliers(BatchWithMask original, List<Outlier> outliers, String top5Deviation) {
        super(original.batchId, original.printId, original.tileId,
                original.layer, original.temp, original.validMask, original.saturated);
        this.outliers = outliers;
        this.top5Deviation = top5Deviation;
    }

}
