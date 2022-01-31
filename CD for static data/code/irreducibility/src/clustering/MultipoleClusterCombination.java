package clustering;

import bounding.CorrelationBounding;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;

public class MultipoleClusterCombination extends ClusterCombination {

    ArrayList<Cluster> clusters;


    public MultipoleClusterCombination(ArrayList<Cluster> clusters) {
        super();
        this.isMultipoleCandidate = true;
        this.clusters = clusters;

        int lastID = clusters.get(0).getClusterId();
        for (int i = 1; i < clusters.size(); i++) {
            int ID = clusters.get(i).getClusterId();
            if (ID < lastID) {
                isDuplicate = true;
                break;
            } else {
                lastID = ID;
            }
        }
        this.checkAndSetMaxSubsetLowerBound(0); // multipole is in the range [0,1]
    }



    public ArrayList<Cluster> getClusters() {
        return this.clusters;
    }




    public double getMaxSubsetCorr(CorrelationBounding CB) {
        ArrayList<Cluster> clusters = this.getClusters();
        ArrayList<Cluster> subset;

        double subsetcorr;
        double max=0;
        for(int i=0; i<clusters.size(); i++){
            subset = (ArrayList<Cluster>) clusters.clone();
            subset.remove(i);
            MultipoleClusterCombination cc = new MultipoleClusterCombination(subset);
            CB.calcBound(cc, true);
            subsetcorr = cc.getLB();
//            if(cc.getNComparisons() > 1){
//                System.err.println("MultipoleClusterCombination.java: this should only be called on sets of clusters of size 1");
//            }

            max = Math.max(subsetcorr, max);

        }
        return max;
    }

    public ArrayList<ClusterCombination> getSplittedCC() {

        //return object:
        ArrayList<ClusterCombination> newCombinations = new ArrayList<>();

        // find cluster with largest radius; we will break this cluster
        int cToBreak = 0;
        double maxDist = -Double.MAX_VALUE;
        for(int i=0; i<clusters.size(); i++){
            if(clusters.get(i).getMaxDist() >= maxDist){
                cToBreak = i;
                maxDist = clusters.get(i).getMaxDist();
            }
        }

        //if we have duplicates, it can happen that maxDist = 0; in that case, break the cluster that contains more than one point:

        if(maxDist<=0){
            for(int i =0; i<clusters.size(); i++){
                if(clusters.get(i).getNPoints()>1){
                    cToBreak=i;
                    break;
                }
            }
        }


        ArrayList<Cluster> newClusters = new ArrayList<>(clusters);

        Cluster largest = newClusters.remove(cToBreak); // remove largest radius cluster
        for (Cluster sc : largest.getSubClusters()) {
            //generate new candidates by replacing the position of largest cluster with its subcluster
            if(newClusters.contains(sc) && sc.listOfContents.size()==1){ // prevent duplicates, only larger id SCs are admittable than the largest cluster already present
                break;
            }

            newClusters.add(cToBreak, sc);


            // this copying is necessary as otherwise we'll remove the cToBreak IN PLACE within the newCandidate object
            MultipoleClusterCombination newCandidate = new MultipoleClusterCombination(new ArrayList<>(newClusters));
            newCandidate.checkAndSetLB(this.getLB()); newCandidate.checkAndSetUB(this.getUB());
            newCombinations.add(newCandidate);

            // remove the subcluster to make room for the next subcluster
            newClusters.remove(cToBreak);
            if(newClusters.contains(sc)){ // prevent duplicates, only larger id SCs are admittable than the largest cluster already present
                break;
            }
        }

        //all new candidates have been added to the newCombinations list

        return newCombinations;

    }


    public ArrayList<ClusterCombination> getSingletonClusters() {
        ArrayList<ClusterCombination> out = new ArrayList<>();

        if (this.canBeSplit()) {

            ArrayList<ClusterCombination> splitted = this.getSplittedCC();

            for (ClusterCombination sc : splitted) {

                out.addAll(sc.getSingletonClusters());


            }


        }else{
            out.add(this);
        }

        return out;

    }


    public ArrayList<Cluster> getLHS() {
        System.err.println("the multipole metric does not distinguish between a left and right hand side");
        return null;
    }


    public ArrayList<Cluster> getRHS() {
        System.err.println("the multipole metric does not distinguish between a left and right hand side");
        return null;
    }
}


