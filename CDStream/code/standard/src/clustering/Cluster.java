package clustering;

import _aux.lib;
import streaming.DCC;
import streaming.TimeSeries;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Cluster {
    // copied and adapted from O. Papapetrou code
//    public static int distType=2; // 1 = l1, 2=l2 // warning: in the current code this does not change anythin
    double maxDist=0;
    Integer clusterId;
    public ArrayList<Integer> listOfContents = new ArrayList<>();
    TimeSeries centroid;
    int[][] centroidHash;
    static boolean maintainCentroid=false;
    ArrayList<int[][]> vectorHashes;
    Cluster parent;
    ArrayList<Cluster> subClusters;
    int level; // for hierarchical clustering
    ArrayList<Double> distances;
    public double maxAngle;
    public double diameter;
    public ConcurrentLinkedQueue<DCC> dccs;
    public int creationTime;
    public double parentSizeRatio;
    public boolean active;
    public int ccCount;
    public int oldId;

    public ArrayList<Double> getSortedDistances() {
        return sortedDistances;
    }

    ArrayList<Double> sortedDistances;
    double avgDist=0;

    public Cluster(int clusterId, TimeSeries centroid, int creationTime) {
        this.setClusterId(clusterId);
        this.creationTime = creationTime;
        this.listOfContents.add(centroid.id);
        this.centroid = centroid;
        this.vectorHashes = new ArrayList<>();
        this.distances = new ArrayList<>();
        this.sortedDistances = new ArrayList<>();
        this.dccs = new ConcurrentLinkedQueue<>();
        this.active = true;
        this.ccCount = 0;
    }

    public int getBranchSize(){
        if (this.hasChildren()){
            int size = this.subClusters.size();
            for (Cluster sc: subClusters){
                size += sc.getBranchSize();
            }
            return size;
        } else {
            return 0;
        }
    }

    public int getBranchDccCount(){
        int size = this.dccs.size();
        if (this.hasChildren()) {
            for (Cluster sc : subClusters) {
                size += sc.getBranchDccCount();
            }
        }
        return size;
    }

    public int getBranchDccSize(){
        Optional totSize = this.dccs.stream().map(x -> x.getSize()).reduce((a,b) -> a+b);
        int finalSize = totSize.isPresent() ? (int) totSize.get() : 0;

        if (this.hasChildren()) {
            for (Cluster sc : subClusters) {
                finalSize += sc.getBranchDccSize();
            }
        }
        return finalSize;
    }

    public int getUpdateCount(TimeSeries[] timeSeries){
        return listOfContents.stream().mapToInt(id -> timeSeries[id].w).sum();
    }

    public void addDCC(DCC dcc){this.dccs.add(dcc);}
    public void removeDCC(DCC dcc){this.dccs.remove(dcc);}

    public boolean hasChildren() {
        return subClusters!=null;
    }
    public void computeAvgDistance() {
        Optional<Double> sum = distances.stream().reduce(Double::sum);
        if (sum.isPresent()) avgDist=sum.get()/distances.size();
        else avgDist=0;
    }
    public void sortDistances() {
        sortedDistances.addAll(distances);
        Collections.sort(sortedDistances);
    }

    public double computeDiameter(TimeSeries[] timeSeries, int m){
        double diameter = listOfContents.size() == 1 ? 0d: 1e-10d;
        int di = 0;
        int dj = 0;
        for (int i: listOfContents){
            for (int j: listOfContents){
                if (i<j){
                    double dist = lib.corrToDist(timeSeries[i].pairwiseCorrelations[j], m);
                    if (dist > diameter){
                        diameter = dist;
                    }
                }
            }
        }
        this.diameter = diameter;
        return diameter;
    }

    public void setDiameter(double diameter){this.diameter = diameter;}

    public double getMaxDist() {
        return maxDist;
    }
    public double getAvgDist() {
        return avgDist;
    }

    public void checkAndSetMaxDist(double dist) {
        maxDist = Math.max(maxDist,dist);
    }
    public void overwriteMaxDist(double reset_value){this.maxDist = reset_value;}

    public void addItem(TimeSeries vector) {
        this.listOfContents.add(vector.id);
        if (maintainCentroid) {
            addOverwrite(centroid, vector);
        }
    }
    public TimeSeries getCentroid() {
        return centroid;
    }

    public void setCentroid(TimeSeries vec){this.centroid = vec;}

    public static void addOverwrite(TimeSeries target, TimeSeries in) {
        for (int i=0;i<target.slidingData.length;i++) {
            target.slidingData[i]+=in.slidingData[i];
        }
    }
    public static double[] addNew(double[] target, double[] in) {
        double[] newVec = target.clone();
        for (int i=0;i<newVec.length;i++) newVec[i]+=in[i];
        return newVec;
    }
    public static double[] addNew(double[] target, double[] in1, double[] in2) {
        double[] newVec = target.clone();
        for (int i=0;i<newVec.length;i++) newVec[i]+=in1[i] + in2[i];
        return newVec;
    }
    public static void divideOverwrite(double[] target, double in) {
        for (int i=0;i<target.length;i++) target[i]/=in;
    }
    public static double[] divideNew(double[] target, double in) {
        double[] newVec = target.clone();
        for (int i=0;i<newVec.length;i++) newVec[i]/=in;
        return newVec;
    }

    public int[][] getCentroidHash() {
        return centroidHash;
    }

    public void setCentroidHash(int[][] centroidHash) {
        this.centroidHash = centroidHash;
    }

    public void addVectorHash(int[][] vecHash){
        this.vectorHashes.add(vecHash);
    }

    public double getDistance(TimeSeries vec){
        return lib.corrToDist(vec.pairwiseCorrelations[centroid.id], vec.slidingData.length);
    }
    public void setParent(Cluster c){
        this.parent = c;
        this.parentSizeRatio = (double) this.listOfContents.size() / parent.listOfContents.size();
    }

    public Cluster getParent(){return this.parent;}

    public int getClusterId(){
        return clusterId;
    }

    public void setClusterId(int id){
        if (this.clusterId == null){
            this.oldId = id;
        } else{
            this.oldId = this.clusterId;
        }
        this.clusterId = id;
    }

    public void setSubClusters(ArrayList<Cluster> subClusters){
        this.subClusters = subClusters;
    }

    public ArrayList<Cluster> getSubClusters(){ return this.subClusters;}

    public int getLevel(){return this.level;}

    public void setLevel(int level){this.level = level;}

    public int getSize(){return this.listOfContents.size();}

    public double getQ3Dist(){
        Collections.sort(this.distances);
        int id = (int) Math.floor(this.distances.size() * 0.75);
        return this.distances.get(id);
    }
}