package clustering;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.Setter;
import similarities.DistanceFunction;
import streaming.TimeSeries;
import streaming.index.ExtremaPair;

import java.util.BitSet;

public class Cluster {
//    Identifiers
    @Setter @Getter public int id;
    private DistanceFunction distFunc;
    private int hashCode = 0;

    //    Points
    public FastLinkedList<Integer> tmpPointsIdx = new FastLinkedList<>();
    public FastArrayList<Integer> pointsIdx;
    public boolean finalized = false;

//    Hypersphere statistics
    @Setter @Getter private Double radius;
    @Getter private TimeSeries centroid;
    public Integer centroidId;

    //    Relations
    @Setter @Getter public Cluster parent;
    @Getter public FastLinkedList<Cluster> children = new FastLinkedList<>();
    @Setter @Getter public int level;

    //    Misc
    public Double score;

    // For sim functions that include norms (e.g. euclidean similarity)
    private double[] normBounds;

    public Cluster(DistanceFunction distFunc, int centroidId) {
        this.centroidId = centroidId;
        this.distFunc = distFunc;
        this.tmpPointsIdx = new FastLinkedList<>();
        tmpPointsIdx.add(centroidId);
    }

    public int hashCode(){
        if (this.hashCode == 0){
            this.hashCode = this.pointsIdx.hashCode();
        }
        return this.hashCode;
    }

    public int size() {
        return pointsIdx.size();
    }
    public int getSize() {
        return pointsIdx.size();
    }

    public boolean contains(Integer o) {
        if (finalized) {
            return pointsIdx.contains(o);
        } else {
            throw new RuntimeException("Cluster is not finalized, not allowed to call contains yet");
        }
    }

    public String toString(){
        return String.format("%d -> %s", id, pointsIdx.toList().toString());
    }

    public boolean equals(Object o){
        if (this == o) return true;
        if (!(o instanceof Cluster)) return false;
        Cluster c = (Cluster) o;
        return c.id == this.id;
    }

    public int get(int i) {
        return pointsIdx.get(i);
    }

    public void addPoint(int i){
        if (finalized) throw new RuntimeException("Cannot add points to a finalized cluster");
        tmpPointsIdx.add(i);
    }

    public void addChild(Cluster sc){
        this.children.add(sc);
    }

    public FastArrayList<TimeSeries> getPoints(TimeSeries[] data){
        if (!finalized) throw new RuntimeException("Cannot get points from a non-finalized cluster");
        FastArrayList<TimeSeries> points = new FastArrayList<>(pointsIdx.size());
        for (Integer i: this.pointsIdx) {
            points.add(data[i]);
        }
        return points;
    }

//    Compute radius
    public void computeRadius(TimeSeries[] data, double[][] pairwiseDistances){
        double maxDist = 0;

//        Remove floating point error
        if (pointsIdx.size() == 1){
            radius = 0.0;
            return;
        }

//        If using geometric Centroid, re-initialize all distances to centroid, otherwise, just pick largest distance from cached distances
        for (int i = 0; i < pointsIdx.size(); i++) {
            maxDist = Math.max(maxDist, pairwiseDistances[centroidId][pointsIdx.get(i)]);
        }

        radius = maxDist;
    }


    public void finalize(TimeSeries[] data, double[][] pairwiseDistances){
        if (finalized) throw new RuntimeException("Cluster already finalized");
        finalized = true;

//        Create final content array
        this.pointsIdx = new FastArrayList<>(tmpPointsIdx);

//        Initialize actual centroid
        centroid = data[centroidId];

//        Compute distances from centroid and determine radius
        computeRadius(data, pairwiseDistances);
    }

    public double getDistance(int pId, double[][] pairwiseDistances){
//        If cluster is not final, compute distance from point centroid, otherwise get from local cache (geometric centroid)
        return pId == centroidId ? 0: pairwiseDistances[pId][centroidId];
    }

    public Double getScore(){
        if (score==null) {
            score = this.radius / this.size();
        }
        return score;
    }

    public double[] getNormBounds(double[][] pairwiseDistances){
        if ( normBounds == null){
            double lb = Double.MAX_VALUE;
            double ub = 0;
            for (Integer i: pointsIdx){
                double normSq = pairwiseDistances[i][i];
                lb = Math.min(lb,normSq);
                ub = Math.max(ub,normSq);
            }
            normBounds = new double[]{lb,ub};
        }
        return normBounds;
    }
}
