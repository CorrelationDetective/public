package clustering;

import _aux.lib;
import bounding.CorrelationBounding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

public class Cluster {
    // copied and adapted from O. Papapetrou code
//    public static int distType=2; // 1 = l1, 2=l2 // warning: in the current code this does not change anything
    double maxDist=0;
    int clusterId;
    public ArrayList<Integer> listOfContents = new ArrayList<>();
    int centroid;
    int[][] centroidHash;
    static boolean maintainCentroid=false;
    ArrayList<int[][]> vectorHashes;
    Cluster parent;
    ArrayList<Cluster> subClusters;
    int level; // for hierarchical clustering
    ArrayList<Double> distances;
    int largestSubClusterID;

    public ArrayList<Double> getSortedDistances() {
        return sortedDistances;
    }

    ArrayList<Double> sortedDistances;
    double avgDist=0;

    public boolean hasChildren() {
        return subClusters!=null;
    }

    public Cluster(int clusterId, int centroid) {
        this.clusterId=clusterId;
        this.listOfContents.add(centroid);
        this.centroid = centroid;
        this.vectorHashes = new ArrayList<>();
        this.distances = new ArrayList<>();
        this.sortedDistances = new ArrayList<>();
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

    public void addItem(int itemId) {
        this.listOfContents.add(itemId);
    }
    public int getCentroid() {
        return centroid;
    }

    public void setCentroid(int centroidID){this.centroid = centroidID;}

    public static void addOverwrite(double[] target, double[] in) {
        for (int i=0;i<target.length;i++) target[i]+=in[i];
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

    public double getDistance(int i, CorrelationBounding CB){
        double dist = lib.getEuclideanFromCorrelation(CB.pointCorrelations[i][centroid], CB.n_dim);
        return dist;
    }
    public void setParent(Cluster c){this.parent = c;}

    public Cluster getParent(){return this.parent;}

    public int getClusterId(){
        return clusterId;
    }

    public void setClusterId(int id){this.clusterId = id;}

    public void setSubClusters(ArrayList<Cluster> subClusters){
        this.subClusters = subClusters;
    }

    public ArrayList<Cluster> getSubClusters(){ return this.subClusters;}

    public int getLevel(){return this.level;}

    public void setLevel(int level){this.level = level;}

    public double getQ3Dist(){
        Collections.sort(this.distances);
        int id = (int) Math.floor(this.distances.size() * 0.75);
        return this.distances.get(id);
    }

    public int getNPoints(){
        return this.listOfContents.size();
    }

    public void setLargestSubClusterID(int ID){
        this.largestSubClusterID = ID;
    }

    public int getLargestSubClusterID(){
        return this.largestSubClusterID;
    }

    public double calcAndGetCorrelationDiameter(CorrelationBounding CB){
        double minCorr = 1;
        for(int i = 0; i<listOfContents.size(); i++){
            int id1 = listOfContents.get(i);
            for(int j = i+1; j<listOfContents.size(); j++){
                int id2 = listOfContents.get(j);
                minCorr = Math.min(minCorr, CB.pointCorrelations[id1][id2]);
            }
        }
        return minCorr;
    }

}