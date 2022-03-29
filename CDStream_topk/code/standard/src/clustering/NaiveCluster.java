package clustering;

import streaming.TimeSeries;

import java.util.ArrayList;


public class NaiveCluster {

    public static ArrayList<Cluster> getKMeansMaxClusters(TimeSeries[] timeSeries, double threshold, int numberOfClusters, int t) {
        // allstocks -> stock dataset in array of size (n_dim rows, n_vec columns);
        // threshold -> epsilon -- max dist from centroid to point in cluster

        ArrayList<Cluster> allClusters = new ArrayList<>(numberOfClusters);
        for (int i = 0; i< timeSeries.length; i++) {
            TimeSeries ts = timeSeries[i];
            Cluster minCluster=null;
            double minDist=Double.MAX_VALUE;
            for (Cluster c:allClusters) {
                double dist = c.getDistance(ts);
                if (dist <= minDist) {
                    minDist = dist;
                    minCluster = c;
                }
            }
            if (minDist<threshold) { // no need to create a new
                minCluster.addItem(ts);
                minCluster.checkAndSetMaxDist(minDist);
            } else if (allClusters.size()<numberOfClusters) { // i can create a new cluster
                Cluster c = new Cluster(allClusters.size(), ts, t);
                c.distances.add(0.0);
                allClusters.add(c);
            } else { //I cannot create a new cluster  -- just assign to the closest
                minCluster.addItem(ts);
                minCluster.checkAndSetMaxDist(minDist);
            }
        }
        for (Cluster c:allClusters) {
            c.computeAvgDistance();
            c.sortDistances();
        }
        return allClusters;
    }

    public static ArrayList<Cluster> getNearDuplicates(TimeSeries[] timeSeries, double threshold, int t) {
        // allstocks -> stock dataset in array of size (n_dim rows, n_vec columns);
        // threshold -> epsilon -- max dist from centroid to point in cluster
        ArrayList<Cluster> allClusters = new ArrayList<>();
        for (TimeSeries ts : timeSeries) {
            Cluster minCluster=null;
            double minDist=threshold;
            for (Cluster c:allClusters) {
                double dist = c.getDistance(ts);
                if (dist <= minDist) {
                    minDist = dist;
                    minCluster = c;
                }
            }
            if (minCluster!=null) {
                minCluster.addItem(ts);
                minCluster.checkAndSetMaxDist(minDist);
            } else {
                Cluster c = new Cluster(allClusters.size(), ts, t);
                c.setCentroid(ts);
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