package clustering;


import _aux.lib;
import bounding.CorrelationBounding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;


public class RecursiveClustering {
    public boolean useKMeans;
    public int breakFirstKLevelsToMoreClusters;
    public int maxLevels;
    public int defaultDesiredClusters;
    public int globalClustID;
    public int clusteringRetries = 10;

    public ArrayList<ArrayList<Cluster>> clustersPerLevel;
    public ArrayList<Cluster> allClusters;
    public int n_vec; public int n_dim;
    public CorrelationBounding CB;
    Random rand;

    public RecursiveClustering(double[][] data, CorrelationBounding CB, int maxLevels, int defaultDesiredClusters, boolean useKMeans, int breakFirstKLevelsToMoreClusters, int clusteringRetries){
        this.clusteringRetries=clusteringRetries;
        this.breakFirstKLevelsToMoreClusters=breakFirstKLevelsToMoreClusters;
        this.useKMeans=useKMeans;
        this.maxLevels = maxLevels;
        this.defaultDesiredClusters = defaultDesiredClusters;
        this.clustersPerLevel = new ArrayList<>();
        for (int i = 0; i <= maxLevels + 1; i++) {
            this.clustersPerLevel.add(new ArrayList<>());
        }
        this.allClusters = new ArrayList<>();
        this.n_vec = data.length; this.n_dim = data[0].length;  //  row = vec, col = dim
        this.CB = CB;
        rand = new Random();
        rand.setSeed(42);

    }

    public void fitRecursiveClusters(double startEpsilon, double epsilonMultiplier){
        Cluster collection = new Cluster(0, 0); // i make this cluster just for being able to reuse the code -- i don't save it somewhere
        this.globalClustID++;
        collection.checkAndSetMaxDist(lib.getEuclideanFromCorrelation(-1, n_dim));
        for (int i=1;i<n_vec;i++)
            collection.addItem(i);
        collection.setLevel(0);
        this.clustersPerLevel.get(0).add(collection);
        this.allClusters.add(collection);
        ArrayList<Cluster> clusters = this.makeAndGetSubClusters(collection, startEpsilon, clusteringRetries);


        for (Cluster c : clusters) {
            c.setParent(collection);
            c.setLevel(1);
            this.clustersPerLevel.get(1).add(c);
            this.allClusters.add(c);
            c.setClusterId(globalClustID);
            this.hierarchicalClustering(c, epsilonMultiplier);
            c.setLargestSubClusterID(globalClustID);
            globalClustID++;
        }
        collection.setLargestSubClusterID(globalClustID);
    }

    public void hierarchicalClustering(Cluster c, double epsilonMultiplier) {
        // top clustering layer:

        int level = c.getLevel();
        double distThreshold;
        if (level < maxLevels) {
            distThreshold = c.getMaxDist() * epsilonMultiplier;
//            distThreshold = c.getQ3Dist();
        } else { // just make a level with individual points
            distThreshold = 0;
        }

        ArrayList<Cluster> subClusters = makeAndGetSubClusters(c, distThreshold, clusteringRetries);
        for (Cluster sc : subClusters) {
            globalClustID++;
            sc.setLevel(level + 1);
            sc.setClusterId(globalClustID);
            this.clustersPerLevel.get(level + 1).add(sc);
            this.allClusters.add(sc);
            if (level < maxLevels && sc.listOfContents.size() > 1) {
                this.hierarchicalClustering(sc, epsilonMultiplier);
            }
            sc.setLargestSubClusterID(globalClustID);
        }



    }

    ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon, int n_random_tries) {
        return makeAndGetSubClusters(c, epsilon,  n_random_tries, false);
    }

    ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon, int n_random_tries, boolean print) {
        ArrayList<Integer> idx = c.listOfContents;

        ArrayList<Cluster> subClusters;
        ArrayList<Integer> bestOrder = null;
        ArrayList<Cluster> bestSubCluster = null;
        double bestDistance = Double.MAX_VALUE;
        // make subclusters of this data with the smaller epsilon. do n_random_tries attempts to get a good clustering
        for (int i = 0; i < n_random_tries; i++) {
            Collections.shuffle(idx, rand);
//            Collections.reverse(idx);
            // get the actual vector data that is part of these clusters:
            int numDesiredClusters=this.defaultDesiredClusters;
            if (epsilon<=0||c.level==maxLevels) numDesiredClusters=idx.size();

            if (c.level<breakFirstKLevelsToMoreClusters) numDesiredClusters*=5;
            if (useKMeans)
                subClusters = NaiveCluster.getKMeansMaxClusters(idx, CB, epsilon, numDesiredClusters);
            else
                subClusters = NaiveCluster.getNearDuplicates(idx, CB, epsilon);


            Optional<Double> score = subClusters.stream().map(sc -> Math.pow(sc.getMaxDist(), 1)/sc.getNPoints()).reduce(Double::sum);
            if (score.get().doubleValue()<bestDistance) {
//                System.err.print("\nDropping distance from " + bestDistance);
                bestSubCluster = (ArrayList<Cluster>) subClusters.clone();
                bestDistance=score.get().doubleValue();
                bestOrder = (ArrayList<Integer>) idx.clone();
//                System.err.print(" to " + bestDistance +" at repetition " + i);
            }
        }
        subClusters = bestSubCluster;
        idx = bestOrder;

        if (print) {
            System.err.println("Best distance is " + bestDistance);
        }
        // we need to correct the list of contents to the right indices of the original data. Also add the parent cluster.
        for (Cluster sc : subClusters) {
            sc.setParent(c);

            if (sc.listOfContents.size() == 1) {
                sc.overwriteMaxDist(0); // remove floating point error

            }
        }


        // finally, add the subclusters to the parent cluster c so we can find them back.
        c.setSubClusters(subClusters);

        return subClusters;
    }

}
