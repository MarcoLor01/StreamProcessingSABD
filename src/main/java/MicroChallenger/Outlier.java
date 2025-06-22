package MicroChallenger;

// Classe helper per memorizzare i risultati
public class Outlier {
    public final int x;
    public final int y;
    public final double deviation;

    public Outlier(int x, int y, double deviation) {
        this.x = x;
        this.y = y;
        this.deviation = deviation;
    }

    @Override
    public String toString() {
        return String.format("P=(%d, %d), dev=%.2f", x, y, deviation);
    }
}
