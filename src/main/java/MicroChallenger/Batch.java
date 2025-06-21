package MicroChallenger;

import java.io.Serializable;

public class Batch implements Serializable {
    public final int batchId;
    public final String printId;
    public final int tileId;
    public final int layer;
    public final int[][] temp;

    public Batch(int batchId, String printId, int tileId, int layer, int[][] temp) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layer = layer;
        this.temp = temp;
    }
    @Override
    public String toString(){
        int firstTempValue = (temp != null && temp.length > 0) ? (temp[0][0] & 0xFFFF) : -1;
        int a = (temp != null && temp.length > 0) ? (temp[1][0] & 0xFFFF) : -1;
        int b = (temp != null && temp.length > 0) ? (temp[0][1] & 0xFFFF) : -1;
        return "BatchRecord{" +
                "batchId=" + batchId +
                ", printId='" + printId + '\'' +
                ", tileId=" + tileId +
                ", layer=" + layer +
                ", tempSize=" + (temp != null ? temp.length : 0) +
                ", temp[0][0]=" + firstTempValue +
                ", temp[1][0]=" + a +
                ", temp[0][1]=" + b +
                '}';
    }

}
