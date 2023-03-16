package bounding;

import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import streaming.index.ExtremaPair;

public class ClusterPair {
    @NonNull public final Cluster c1;
    @NonNull public final Cluster c2;

    @Getter @Setter private ExtremaPair lbDistExtrema;
    @Getter @Setter private ExtremaPair ubDistExtrema;
    @Getter @Setter private double[] bounds;


    public ClusterPair(Cluster c1, Cluster c2, double[] bounds, ExtremaPair lbDistExtrema, ExtremaPair ubDistExtrema) {
        if (c1.id < c2.id) {
            this.c1 = c1;
            this.c2 = c2;
        } else {
            this.c1 = c2;
            this.c2 = c1;
        }
        this.bounds = bounds;
        this.lbDistExtrema = lbDistExtrema;
        this.ubDistExtrema = ubDistExtrema;
    }

    public String toString(){
        return c1.id + "," + c2.id;
    }

    public int hashCode(int totalClusters) {
        return c1.id * totalClusters + c2.id;
    }
}
