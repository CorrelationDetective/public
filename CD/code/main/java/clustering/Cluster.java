package clustering;

import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.Setter;
import similarities.DistanceFunction;

import java.util.*;

public class Cluster {
    @Setter @Getter public int id;
    private DistanceFunction distFunc;

    //    Points
    public final boolean geoCentroid;
    public FastLinkedList<Integer> tmpPointsIdx = new FastLinkedList<>();
    public FastArrayList<Integer> pointsIdx;
    @Getter HashMap<Integer, Double> distances;
    public boolean finalized = false;

//    Hypersphere statistics
    @Setter @Getter private Double radius;
    @Getter private double[] centroid;
    public Integer centroidIdx;

    //    Relations
    @Setter @Getter public Cluster parent;
    @Getter public FastLinkedList<Cluster> children = new FastLinkedList<>();
    @Setter @Getter public int level;

    //    Misc
    public Double score;

    //    For total correlation only currently (for saving entropy bounds)
    private double[] entropyBounds;

    public Cluster(DistanceFunction distFunc, int centroidIdx, boolean geoCentroid) {
        this.centroidIdx = centroidIdx;
        this.distFunc = distFunc;
        this.tmpPointsIdx = new FastLinkedList<>();
        tmpPointsIdx.add(centroidIdx);
        this.geoCentroid = geoCentroid;
    }

    public int size() {
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
        return Integer.toString(id);
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

    public FastArrayList<double[]> getPoints(double[][] data){
        if (!finalized) throw new RuntimeException("Cannot get points from a non-finalized cluster");
        FastArrayList<double[]> points = new FastArrayList<>(pointsIdx.size());
        for (Integer i: this.pointsIdx) {
            points.add(data[i]);
        }
        return points;
    }

//    Compute radius
    public void computeRadius(double[][] data, double[][] pairwiseDistances){
        double maxDist = 0;

//        Remove floating point error
        if (pointsIdx.size() == 1){
            radius = 0.0;
            return;
        }

//        If using geometric Centroid, re-initialize all distances to centroid, otherwise, just pick largest distance from cached distances
        if (geoCentroid){
            distances = new HashMap<>(pointsIdx.size());
            for (int i = 0; i < pointsIdx.size(); i++) {
                double dist = distFunc.dist(data[pointsIdx.get(i)], this.getCentroid());
                distances.put(pointsIdx.get(i), dist);
                maxDist = Math.max(maxDist, dist);
            }
        } else {
            for (int i = 0; i < pointsIdx.size(); i++) {
                maxDist = Math.max(maxDist, pairwiseDistances[centroidIdx][pointsIdx.get(i)]);
            }
        }

        radius = maxDist;
    }

//    Compute centroid
    public void computeGeometricCentroid(double[][] data) {
        centroid = lib.elementwiseAvg(getPoints(data));
        centroidIdx = null;
    }

    public void finalize(double[][] data, double[][] pairwiseDistances){
        if (finalized) throw new RuntimeException("Cluster already finalized");
        finalized = true;

//        Create final content array
        this.pointsIdx = new FastArrayList<>(tmpPointsIdx);
        this.distances = new HashMap<>(tmpPointsIdx.size());

//        Initialize actual centroid
        if (geoCentroid) {
            computeGeometricCentroid(data);
        } else {
            centroid = data[centroidIdx];
        }

//        Compute distances from centroid and determine radius
        computeRadius(data, pairwiseDistances);
    }

    public double getDistance(int pId, double[][] pairwiseDistances){
//        If cluster is not final, compute distance from point centroid, otherwise get from local cache (geometric centroid)
        if (finalized && geoCentroid){
            return distances.get(pId);
        } else {
            return pairwiseDistances[pId][centroidIdx];
        }
    }

    public Double getScore(){
        if (score==null) {
            score = this.radius / this.size();
        }
        return score;
    }

    public double[] getEntropyBounds(double[][] pairwiseEntropies){
        if (entropyBounds == null){
            double lb = Double.MAX_VALUE;
            double ub = 0;
            for (Integer i: pointsIdx){
                double e = pairwiseEntropies[i][i];
                lb = Math.min(lb,e);
                ub = Math.max(ub,e);
            }
            entropyBounds = new double[]{lb,ub};
        }
        return entropyBounds;
    }
}
