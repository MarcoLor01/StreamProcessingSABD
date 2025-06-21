package MicroChallenger;

public class BatchWithMask extends Batch {
    public final boolean[][] validMask;
    public final int saturated;

    public BatchWithMask(int batchId, String printId, int tileId, int layer, int[][] temp, boolean[][] validMask, int saturated) {
        super(batchId, printId, tileId, layer, temp);
        this.validMask = validMask;
        this.saturated = saturated;
    }
}
