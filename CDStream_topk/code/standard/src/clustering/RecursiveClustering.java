package clustering;


import bounding.CorrelationBounding;
import streaming.TimeSeries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;


public class RecursiveClustering {
    public boolean useKMeans;
    public int breakFirstKLevelsToMoreClusters;
    public TimeSeries[] timeSeries;
    public int maxLevels;
    public int defaultDesiredClusters;
    public int globalClustID;
    public int clusteringRetries = 10;
    public Random random;

    public ArrayList<ArrayList<Cluster>> clustersPerLevel;
    public ArrayList<Cluster> allClusters;
    public int n_vec; public int n_dim;
    public CorrelationBounding CB;

    public RecursiveClustering(TimeSeries[] timeSeries, int maxLevels, int defaultDesiredClusters, boolean useKMeans, int breakFirstKLevelsToMoreClusters, int clusteringRetries, int seed){
//        System.err.println("Clustering retries are " + clusteringRetries);
        this.clusteringRetries=clusteringRetries;
        this.breakFirstKLevelsToMoreClusters=breakFirstKLevelsToMoreClusters;
        this.useKMeans=useKMeans;
        this.maxLevels = maxLevels;
        this.timeSeries = timeSeries;
        this.defaultDesiredClusters = defaultDesiredClusters;
        this.clustersPerLevel = new ArrayList<>();
        for (int i = 0; i <= maxLevels + 1; i++) {
            this.clustersPerLevel.add(new ArrayList<>());
        }
        this.allClusters = new ArrayList<>();
        this.n_vec = timeSeries.length; this.n_dim = timeSeries[0].w;  //  row = vec, col = dim

        this.random = new Random(seed);
//        System.err.println("number of clustering retries is: " + clusteringRetries);
    }

    public void fitRecursiveClusters(double startEpsilon, double epsilonMultiplier, int clusteringRetries, int t){
        Cluster collection = new Cluster(-1, timeSeries[0], t); // i make this cluster just for being able to reuse the code -- i don't save it somewhere
        collection.checkAndSetMaxDist(Double.MAX_VALUE);
        for (int i=1;i<n_vec;i++)
            collection.addItem(timeSeries[i]);
        collection.computeDiameter(timeSeries, n_dim);
        collection.setLevel(0);
        this.clustersPerLevel.get(0).add(collection);
        this.allClusters.add(collection);
        ArrayList<Cluster> clusters = this.makeAndGetSubClusters(collection, startEpsilon, clusteringRetries, t);

        for (Cluster c : clusters) {
            c.setParent(collection);
            c.setLevel(1);
            this.clustersPerLevel.get(1).add(c);
            this.allClusters.add(c);
            c.setClusterId(globalClustID);
            globalClustID++;
            this.hierarchicalClustering(c, epsilonMultiplier, this.clusteringRetries, t);
        }
    }

    public void hierarchicalClustering(Cluster c, double epsilonMultiplier, int clusteringRetries, int t) {
        // top clustering layer:

        int level = c.getLevel();
        double distThreshold;
        if (level < maxLevels) {
            distThreshold = c.getMaxDist() * epsilonMultiplier;
//            distThreshold = c.getQ3Dist();
        } else { // just make a level with individual points
            distThreshold = 0;
        }

        ArrayList<Cluster> subClusters = makeAndGetSubClusters(c, distThreshold, clusteringRetries, t);
        for (Cluster sc : subClusters) {
            sc.setLevel(level + 1);
            sc.setClusterId(globalClustID);
            globalClustID++;
            this.clustersPerLevel.get(level + 1).add(sc);
            this.allClusters.add(sc);
            if (level < maxLevels && sc.listOfContents.size() > 1) {
                this.hierarchicalClustering(sc, epsilonMultiplier, clusteringRetries, t);
            }
        }


    }

    public ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon, int n_random_tries, int t) {
        return makeAndGetSubClusters(c, epsilon,  n_random_tries, false, t);
    }

    ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon, int n_random_tries, boolean print, int t) {
        ArrayList<Integer> idx = c.listOfContents;

        ArrayList<Cluster> subClusters = new ArrayList<>();
        ArrayList<Integer> bestOrder = null;
        ArrayList<Cluster> bestSubCluster = null;
        double bestDistance = Double.MAX_VALUE;
        // make subclusters of this data with the smaller epsilon. do n_random_tries attempts to get a good clustering
        for (int i = 0; i < n_random_tries; i++) {
            Collections.shuffle(idx, this.random);
//            Collections.reverse(idx);
            // get the actual vector data that is part of these clusters:
            TimeSeries[] dataPoints = new TimeSeries[idx.size()];
            for (int pID = 0; pID < idx.size(); pID++) { // pID = index of datapoint index in idx list
                int id = idx.get(pID);
                dataPoints[pID] = this.timeSeries[id];
            }
            int numDesiredClusters=this.defaultDesiredClusters;
            if (epsilon==0||c.level==maxLevels) numDesiredClusters=idx.size();

            if (c.level<breakFirstKLevelsToMoreClusters) numDesiredClusters*=5;

            if (useKMeans)
                subClusters = NaiveCluster.getKMeansMaxClusters(dataPoints, epsilon, numDesiredClusters, t);
            else
                subClusters = NaiveCluster.getNearDuplicates(dataPoints, epsilon, t);

            Optional<Double> distanceOfSolution = subClusters.stream().map(a->a.getMaxDist()*a.listOfContents.size()).reduce(Double::sum);
            if (distanceOfSolution.get().doubleValue()<bestDistance) {
//                System.err.print("\nDropping distance from " + bestDistance);
                bestSubCluster = (ArrayList<Cluster>) subClusters.clone();
                bestDistance=distanceOfSolution.get().doubleValue();
                bestOrder = (ArrayList<Integer>) idx.clone();
//                System.err.print(" to " + bestDistance +" at repetition " + i);
            }
        }
        subClusters = bestSubCluster;

        if (print) {
            System.err.println("Best distance is " + bestDistance);
        }
        // we need to correct the list of contents to the right indices of the original data. Also add the parent cluster.
        for (Cluster sc : subClusters) {
            sc.setParent(c);

            if (sc.listOfContents.size() == 1) {
                sc.overwriteMaxDist(0); // remove floating point error
            }
            sc.computeDiameter(timeSeries, n_dim);

            if (sc.listOfContents.size() > 1 & sc.diameter < 1e-10){
                System.out.println("non-singleton with zero diameter");
            }
        }

        // finally, add the subclusters to the parent cluster c so we can find them back.
        c.setSubClusters(subClusters);

        return subClusters;
    }

    public void setCB(CorrelationBounding CB){this.CB = CB;}

}
