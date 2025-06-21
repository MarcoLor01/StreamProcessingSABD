package MicroChallenger;

public class BatchWithMask extends Batch {
    public final boolean[][] validMask;

    public BatchWithMask(int batchId, String printId, int tileId, int layer, int[][] temp, boolean[][] validMask) {
        super(batchId, printId, tileId, layer, temp);
        this.validMask = validMask;
    }
}
