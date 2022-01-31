package clustering;

import bounding.CorrelationBounding;

import java.util.ArrayList;

public class MultiPearsonClusterCombination extends ClusterCombination{
    ArrayList<Cluster> LHS;
    ArrayList<Cluster> RHS;


    public MultiPearsonClusterCombination(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS){
        super();
        this.LHS = LHS; this.RHS = RHS;
        this.isMultiPearsonCandidate = true;

        for(int i=1; i<LHS.size(); i++){
            if(isDuplicate){
                break;
            }
            Cluster c1 = LHS.get(i-1);
            Cluster c2 = LHS.get(i);
            if(c1.getClusterId() > c2.getClusterId()){ // skip the calculation as we will encounter this case flipped some other time
                isDuplicate=true;
                break;
            }
        }

        for(int i=1; i<RHS.size(); i++){
            if(isDuplicate){
                break;
            }
            Cluster c1 = RHS.get(i-1);
            Cluster c2 = RHS.get(i);
            if(c1.getClusterId() > c2.getClusterId()){ // skip the calculation as we will encounter this case flipped some other time
                isDuplicate=true;
                break;
            }
        }
    }

    public ArrayList<Cluster> getLHS(){
        return this.LHS;
    }

    public ArrayList<Cluster> getRHS(){
        return this.RHS;
    }

    public ArrayList<Cluster> getClusters(){

        ArrayList<Cluster> out = new ArrayList<>();
        out.addAll(LHS); out.addAll(RHS);
        return out;
    }

    public void swapLeftRightSide(){
        ArrayList<Cluster> left = this.LHS;
        ArrayList<Cluster> right = this.RHS;
        this.LHS = right;
        this.RHS = left;
    }

    @Override
    public double getMaxSubsetCorr(CorrelationBounding CB) {
        ArrayList<Cluster> LHS = this.getLHS();
        ArrayList<Cluster> RHS = this.getRHS();
        ArrayList<Cluster> subset;
        //NOTE: we are currently ignoring smaller sized subsets than |candidate|-1
        double subsetcorr;
        double max=-1;

        if(LHS.size() > 1){
            for(int i=0; i<LHS.size(); i++){
                subset = (ArrayList<Cluster>) LHS.clone();
                subset.remove(i);
                MultiPearsonClusterCombination cc = new MultiPearsonClusterCombination(subset, RHS);
                CB.calcBound(cc, true);
                subsetcorr = cc.getLB();
                if(Math.abs(subsetcorr - cc.getUB()) > 0.001){
                    System.err.println("MultipoleClusterCombination.java: getMaxSubsetCorr should only be called on clusters of size 1");
                }

                max = Math.max(subsetcorr, max);
                max = Math.max(max, cc.getMaxSubsetCorr(CB));

            }
        }


        if(RHS.size()>1){
            for(int i=0; i<RHS.size(); i++){
                subset = (ArrayList<Cluster>) RHS.clone();
                subset.remove(i);
                MultiPearsonClusterCombination cc = new MultiPearsonClusterCombination(LHS, subset);
                CB.calcBound(cc, true);
                subsetcorr = cc.getLB();
                if(Math.abs(subsetcorr - cc.getUB()) > 0.001){
                    System.err.println("MultiPearsonClusterCombination.java: this should only be called on clusters of size 1");
                }

                max = Math.max(subsetcorr, max);
                max = Math.max(max, cc.getMaxSubsetCorr(CB));

            }
        }

//        if((RHS.size()>1 || LHS.size()>1)){
//            System.err.println("this seems wrong");
//        }

        return max;

    }

    @Override
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

    public ArrayList<ClusterCombination> getSplittedCC(){

        //todo: we still get duplicates when the lhs and rhs are of equal size. how to fix this?

        //todo: avoid symmetrical duplicates. seems to work for differently-sized lhs and rhs -> probably because the max subset corr is always higher (minJump)!

        //return object:
        ArrayList<ClusterCombination> newCombinations = new ArrayList<>();

        ArrayList<Cluster> allClusters = new ArrayList<>();
        allClusters.addAll(LHS); allClusters.addAll(RHS);

        // find cluster with largest radius; we will break this cluster
        int cToBreak = 0;
        double maxDist = -Double.MAX_VALUE;
        for(int i=0; i<allClusters.size(); i++){
            if(allClusters.get(i).getMaxDist() >= maxDist){
                cToBreak = i;
                maxDist = allClusters.get(i).getMaxDist();
            }
        }

        //if we have duplicate vectors with pearson = 1, it can happen that maxDist = 0; in that case, break the cluster that contains more than one point:

        if(maxDist<=0){
            for(int i =0; i<allClusters.size(); i++){
                if(allClusters.get(i).getNPoints()>1){
                    cToBreak=i;
                    break;
                }
            }
        }

        if(cToBreak < LHS.size()){ // the largest radius is in the left hand side
            ArrayList<Cluster> newLHS = new ArrayList<>(LHS);

            Cluster largest = newLHS.remove(cToBreak); // remove largest radius cluster
            for(Cluster sc : largest.getSubClusters()){
                if(newLHS.contains(sc) && sc.listOfContents.size()==1){
                    break;
                }
                //generate new candidates by replacing the position of largest cluster with its subcluster

                newLHS.add(cToBreak, sc);

                // this copying is necessary as otherwise we'll remove the cToBreak IN PLACE within the newCandidate object
                MultiPearsonClusterCombination newCandidate = new MultiPearsonClusterCombination(new ArrayList<>(newLHS), new ArrayList<>(RHS));
                newCombinations.add(newCandidate);

                // remove the subcluster to make room for the next subcluster
                newLHS.remove(cToBreak);
                if(newLHS.contains(sc)){
                    break;
                }
            }

        }else{ // largest radius is in the RHS:
            ArrayList<Cluster> newRHS = new ArrayList<>(RHS);


            cToBreak -= LHS.size(); // to get the ids for RHS list only

            Cluster largest = newRHS.remove(cToBreak); // remove largest radius cluster

            ArrayList<Cluster> subclusters = largest.getSubClusters();
            if(subclusters == null){
                System.err.println("tried to take subclusters from a singleton cluster");
            }
            for(Cluster sc : subclusters){
                if(newRHS.contains(sc) && sc.listOfContents.size()==1){
                    break;
                }
                //generate new candidates by replacing the position of largest cluster with its subcluster

                newRHS.add(cToBreak, sc);

                // this copying is necessary as otherwise we'll remove the cToBreak IN PLACE within the newCandidate object
                MultiPearsonClusterCombination newCandidate = new MultiPearsonClusterCombination(new ArrayList<>(LHS), new ArrayList<>(newRHS));
                newCandidate.checkAndSetLB(this.getLB()); newCandidate.checkAndSetUB(this.getUB());
                newCombinations.add(newCandidate);

                // remove the subcluster to make room for the next subcluster
                newRHS.remove(cToBreak);
                if(newRHS.contains(sc)){
                    break;
                }
            }

        }

        //we have now put all new combinations in the newCombinations list

        return newCombinations;

    }




}
