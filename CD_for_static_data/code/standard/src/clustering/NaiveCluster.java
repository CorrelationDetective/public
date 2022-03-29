package clustering;

import _aux.lib;
import bounding.CorrelationBounding;

import java.util.ArrayList;
import java.util.List;


public class NaiveCluster {

    public static ArrayList<Cluster> getKMeansMaxClusters(List<Integer> vIDs, CorrelationBounding CB, double threshold, int numberOfClusters) {
        // allstocks -> stock dataset in array of size (n_dim rows, n_vec columns);
        // threshold -> epsilon -- max dist from centroid to point in cluster

        ArrayList<Cluster> allClusters = new ArrayList<>(Math.min(numberOfClusters, 100));
        for (int i : vIDs) {
            Cluster minCluster=null;
            double minDist=Double.MAX_VALUE;
            for (Cluster c:allClusters) {
                double dist = c.getDistance(i,CB);
                if (dist <= minDist) {
                    minDist = dist;
                    minCluster = c;
                }
            }
            if (minDist<threshold) { // no need to create a new
                minCluster.addItem(i);
                minCluster.distances.add(minDist); // only works when setAvgCentroid==false
                minCluster.checkAndSetMaxDist(minDist);

            } else if (allClusters.size()<numberOfClusters) { // i can create a new cluster
                Cluster c = new Cluster(allClusters.size(), i); // note that i is added to the listofcontents in the constructor
                c.setCentroid(i);
                c.distances.add(0.0);
                allClusters.add(c);

            } else { //I cannot create a new cluster  -- just assign to the closest
                minCluster.addItem(i);
                minCluster.distances.add(minDist); // only works when setAvgCentroid==false

                minCluster.checkAndSetMaxDist(minDist);
            }
        }
        for (Cluster c:allClusters) {
            c.computeAvgDistance();
            c.sortDistances();
        }
        return allClusters;
    }

    public static ArrayList<Cluster> getNearDuplicates(List<Integer> vIDs, CorrelationBounding CB, double threshold) {
        // allstocks -> stock dataset in array of size (n_dim rows, n_vec columns);
        // threshold -> epsilon -- max dist from centroid to point in cluster
        ArrayList<Cluster> allClusters = new ArrayList<>();
        for (int i : vIDs) {
            Cluster minCluster=null;
            double minDist=threshold;
            for (Cluster c:allClusters) {
                double dist = c.getDistance(i, CB);
                if (dist <= minDist) {
                    minDist = dist;
                    minCluster = c;
                }
            }
            if (minCluster!=null) {
                minCluster.addItem(i);
                minCluster.distances.add(minDist); // only works when setAvgCentroid==false

                minCluster.checkAndSetMaxDist(minDist);
            } else {
                Cluster c = new Cluster(allClusters.size(), i);
                c.setCentroid(i);
                c.distances.add(0.0);
                allClusters.add(c);
            }
        }
        for (Cluster c:allClusters) {
            c.computeAvgDistance();
        }

        return allClusters;
    }

}