package MicroChallenger;

import java.util.List;

public class TileWithOutliers extends BatchWithMask {
    public final List<Outlier> outliers;

    public TileWithOutliers(BatchWithMask original, List<Outlier> outliers) {
        super(original.batchId, original.printId, original.tileId,
                original.layer, original.temp, original.validMask);
        this.outliers = outliers;
    }
}
