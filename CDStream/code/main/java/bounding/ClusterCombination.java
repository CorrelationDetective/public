package bounding;

import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import core.Parameters;
import queries.ResultTuple;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import streaming.TimeSeries;
import streaming.index.IndexInstruction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.stream.Collectors;

public class ClusterCombination {
//    Identifiers
    public Integer hashCode;
    @NonNull @Getter private FastArrayList<Cluster> LHS;
    @NonNull @Getter private FastArrayList<Cluster> RHS;
    @NonNull @Getter private int level;
    @NonNull private int size;
    private FastArrayList<Cluster> clusters;
    FastArrayList<ClusterCombination> subsetCCs;
    private Boolean isSingleton;

    //    Bounding attributes
    @Setter public Long boundingTimestamp;
    @Setter @Getter private boolean isPositive = false;
    @Setter @Getter private boolean isDecisive = false;
    @Setter @Getter private boolean isDead = false;

//    Boolean indicating if the CC was impacted by an additional constraint (i.e. irreducibility or minjump)
    @Setter @Getter private boolean constrained = false;

//    Boolean indicated if the CC was artificially created (e.g. by topK expansion)
    @Setter @Getter private boolean artificial = false;
    @Getter double LB = -Double.MAX_VALUE;

    @Getter double UB = Double.MAX_VALUE;


    //    Cluster pairs that make up the LB, packaged as IndexInstructions to indicate whether to index the lb or ub extrema pair
    @Getter private FastArrayList<IndexInstruction> lbIndexInstructions;

    //    Cluster pairs that make up the UB, packaged as IndexInstructions to indicate whether to index the lb or ub extrema pair
    @Getter private FastArrayList<IndexInstruction> ubIndexInstructions;

    @Getter double maxPairwiseLB = -Double.MAX_VALUE;
    @Getter @Setter double centerOfBounds = 0.0;
    @Getter double criticalShrinkFactor = Double.MAX_VALUE;
    @Getter Double maxSubsetSimilarity;

    public ClusterCombination(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, int level, int size){
        this.LHS = LHS;
        this.RHS = RHS;
        this.level = level;

//        Number of vector materializations it emcompasses
        if (size < 0){ // happens on int overflow
            this.size = Integer.MAX_VALUE;
        } else {
            this.size = size;
        }
    }


//    ------------------- METHODS -------------------
    public int size() {
        return size;
    }

    public double getSimilarity(){ return getLB(); }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof ClusterCombination))return false;
        return this.hashCode() == other.hashCode();
    }

    public int compareTo(ClusterCombination otherCC){
        return Double.compare(this.getSimilarity(), otherCC.getSimilarity());
    }

    @Override
    public String toString(){
        return LHS.stream().map(c -> Integer.toString(c.getId())).collect(Collectors.joining(",")) + " | " +
                RHS.stream().map(c -> Integer.toString(c.getId())).collect(Collectors.joining(","));
    }

    @Override
    public int hashCode(){
//        If singleton, use timeseries ids indead of cluster ids
        synchronized (this){
            if (hashCode == null){
//                Concatenate hashcodes of clusters
                FastArrayList<Cluster> clusters = this.getClusters();
//                Create hashcode
                this.hashCode = clusters.stream().map(Cluster::hashCode).collect(Collectors.toList()).hashCode();
            }
        }

        return hashCode;
    }

    public void kill(){
        isDead = true;
        isDecisive = false;
        isPositive = false;
    }

    public void clearIndexInstructions(){
        lbIndexInstructions = null;
        ubIndexInstructions = null;
    }

    public boolean containsLeft(int... I){
        FastArrayList<Cluster> tmp = new FastArrayList<>(this.LHS);
        outerloop:
        for (int i : I) {
            for (Cluster c: tmp){
                if (c.contains(i)){
                    tmp.remove(c);
                    continue outerloop;
                }
            }
            return false;
        }
        return true;
    }

    public boolean containsRight(int... I){
        FastArrayList<Cluster> tmp = new FastArrayList<>(this.RHS);
        outerloop:
        for (int i : I) {
            for (Cluster c: tmp){
                if (c.contains(i)){
                    tmp.remove(c);
                    continue outerloop;
                }
            }
            return false;
        }
        return true;
    }

    public ClusterCombination clone() {
        ClusterCombination cc = new ClusterCombination(new FastArrayList<>(this.LHS), new FastArrayList<>(this.RHS), this.level, this.size);
        cc.hashCode = this.hashCode;
        cc.clusters = this.clusters;
        return cc;
    }

    public ClusterCombination getMirror(){
        ClusterCombination cc = new ClusterCombination(new FastArrayList<>(this.RHS), new FastArrayList<>(this.LHS), this.level, this.size);
        cc.setPositive(this.isPositive);
        cc.setDecisive(this.isDecisive);
        cc.checkAndSetBounds(new ClusterBounds(this.LB, this.UB, this.maxPairwiseLB, this.lbIndexInstructions, this.ubIndexInstructions));
        return cc;
    }

    public boolean isSingleton(){
        if (this.isSingleton == null){
            for (Cluster c : this.getClusters()){
                if (c.size() > 1){
                    this.isSingleton = false;
                    return false;
                }
            }
            this.isSingleton = true;
        }
        return this.isSingleton;
    }

    public FastArrayList<Cluster> getClusters(){
        if (this.clusters == null){
            this.clusters = new FastArrayList<>(this.LHS.size() + this.RHS.size());
            this.clusters.addAll(LHS);
            this.clusters.addAll(RHS);
        }
        return this.clusters;
    }

    public void checkAndSetLB(double LB){
        this.LB = Math.max(LB, this.LB);
    }

    public void checkAndSetUB(double UB){
        this.UB = Math.min(UB, this.UB);
    }

    public void checkAndSetMaxPairwiseLB(double maxPairwiseLB){
        this.maxPairwiseLB = Math.max(this.maxPairwiseLB, maxPairwiseLB);
    }

    public double getRadiiGeometricMean(){
        double out = 1;
        for(Cluster c: this.getClusters()){
            out *= c.getRadius();
        }
        return Math.pow(out, 1.0/this.getClusters().size());
    }

    public double getShrunkUB(double shrinkFactor, double maxApproximationSize){
        if(!this.isSingleton() && this.getRadiiGeometricMean() < maxApproximationSize){
            return centerOfBounds + shrinkFactor * (UB - centerOfBounds);
        }else{
            return UB;
        }
    }

    public void setCriticalShrinkFactor(double threshold){
        this.criticalShrinkFactor = (threshold - this.centerOfBounds) / (this.getUB() - this.centerOfBounds);
    }

    public void checkAndSetBounds(ClusterBounds bounds){
        this.LB = bounds.getLB();
        this.UB = bounds.getUB();
        this.lbIndexInstructions = bounds.getLbIndexInstructions();
        this.ubIndexInstructions = bounds.getUbIndexInstructions();

        this.checkAndSetMaxPairwiseLB(bounds.getMaxLowerBoundSubset());
        this.setCenterOfBounds((bounds.getUB() + bounds.getLB()) / 2);
        this.setBoundingTimestamp(System.nanoTime());
    }

    public void sortSides(boolean ascending){
        if (ascending){
            this.LHS.sort((c1, c2) -> Integer.compare(c1.id, c2.id));
            this.RHS.sort((c1, c2) -> Integer.compare(c1.id, c2.id));
        }else{
            this.LHS.sort((c1, c2) -> Integer.compare(c2.id, c1.id));
            this.RHS.sort((c1, c2) -> Integer.compare(c2.id, c1.id));
        }
    }

    public ClusterCombination expand(Cluster c, boolean expandLeft){
        FastArrayList<Cluster> newLHS = new FastArrayList<>(expandLeft ? LHS.size()+1: LHS.size());
        newLHS.addAll(LHS);

        FastArrayList<Cluster> newRHS = new FastArrayList<>(expandLeft ? RHS.size(): RHS.size()+1);
        newRHS.addAll(RHS);

        if (expandLeft) {
            newLHS.add(c);
        } else {
            newRHS.add(c);
        }
        return new ClusterCombination(newLHS, newRHS, this.level + 1, this.size * c.size());
    }

    public ClusterCombination reduce(int i, boolean reduceLeft){
        FastArrayList<Cluster> newLHS = new FastArrayList<>(this.LHS);
        FastArrayList<Cluster> newRHS = new FastArrayList<>(this.RHS);
        Cluster out;
        if (reduceLeft) {
            out = newLHS.remove(i);
        } else {
            out = newRHS.remove(i);
        }
        return new ClusterCombination(newLHS, newRHS, this.level - 1, this.size / out.size());
    }

    public FastArrayList<ClusterCombination> getSubsetCCs(){
        if (this.subsetCCs == null){
            this.subsetCCs = new FastArrayList<>(this.getClusters().size());
            if (this.LHS.size() > 1){
                for (int i = 0; i < this.LHS.size(); i++) {
                    this.subsetCCs.add(this.reduce(i, true));
                }
            }
            if (this.RHS.size() > 1){
                for (int i = 0; i < this.RHS.size(); i++) {
                    this.subsetCCs.add(this.reduce(i, false));
                }
            }
        }
        return this.subsetCCs;
    }

    //    Find the maximum similarity of one of the subsets of this cluster combination
    public double computeMaxSubsetSimilarity(Parameters par){
        if (maxSubsetSimilarity == null){
            double subsetSimilarity;
            maxSubsetSimilarity = -Double.MAX_VALUE;

            for (ClusterCombination subCC : this.getSubsetCCs()){
                par.simMetric.bound(subCC, par.Wl[subCC.getLHS().size() - 1],
                        par.maxPRight > 0 ? par.Wr[subCC.getRHS().size() - 1]: null, par.pairwiseDistances);
                subsetSimilarity = subCC.getLB();
                if (Math.abs(subsetSimilarity - subCC.getUB()) > 0.001){
                    par.LOGGER.fine("Subset similarity is not tight: " + subsetSimilarity + " " + subCC.getUB());
                }

                maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subsetSimilarity);
                maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subCC.computeMaxSubsetSimilarity(par));
            }
        }

        return maxSubsetSimilarity;
    }

//    Split cluster combination into 'smaller' combinations by replacing the largest cluster with its children
    public FastArrayList<ClusterCombination> split(double[] Wl, double[] Wr, boolean allowSideOverlap){


        int lSize = LHS.size();

//        Get cluster with largest radius and more than one point
        int cToBreak = 0;
        double maxRadius = -Double.MAX_VALUE;

        int ctn=0;
        for (Cluster c: this.getClusters()) {
            if (c.size() > 1 && c.getRadius() > maxRadius){// always break leftmost cluster with largest radius
                maxRadius = c.getRadius();
                cToBreak = ctn;
            }
            ctn++;
        }

        boolean isLHS = cToBreak < lSize;

        FastArrayList<Cluster> newSide = new FastArrayList<>(isLHS ? LHS : RHS);
        FastArrayList<Cluster> otherSide = isLHS ? RHS : LHS;
        FastArrayList<Cluster> oldSide = new FastArrayList<>(newSide);

        int scPos = isLHS ? cToBreak : cToBreak - lSize;
        double[] W = isLHS ? Wl : Wr;
        double scWeight = W[scPos];

//        Get closest cluster with same weight for anti-symmetry checks (i.e. B for (A,B,C) after splitting on C)
        Cluster nearDupeLeft = null;
        Cluster nearDupeRight = null;
        for (int i = scPos + 1; i < newSide.size(); i++) {
            if (W[i] == scWeight){
                nearDupeRight = oldSide.get(i);
                break;
            }
        }
        for (int i = scPos - 1; i >= 0; i--) {
            if (W[i] == scWeight){
                nearDupeLeft = oldSide.get(i);
                break;
            }
        }

//        Cluster to split
        Cluster largest = newSide.remove(scPos);

        FastArrayList<ClusterCombination> subCCs = new FastArrayList<>(largest.children.size());

//        For each subcluster, create a new cluster combination  (considering potential sideOverlap and weightOverlap)
        for (Cluster sc : largest.getChildren()) {
//            No same side overlap (e.g. no X | (A,A) ) -- note that this means we also not consider X | (A,B,A), which might be interesting, but out of scope here
//            Ensure by keeping sides in descending order on id
            if (weightOverlapOneSide(sc, nearDupeLeft, nearDupeRight)){
                break; // break because other children will have even larger ids
            }

            //            No same vector on same or both sides
            if ( sc.size() == 1 && (newSide.contains(sc) || (!allowSideOverlap && otherSide.contains(sc)))){
                continue;
            }

            newSide.add(scPos, sc);

            //            Check for two side overlap
            if ((Arrays.equals(Wl, Wr) && (isLHS ? newSide.get(0).id < otherSide.get(0).id : otherSide.get(0).id < newSide.get(0).id))){ // No weight overlap
                newSide.remove(scPos);
                continue;
            }

            FastArrayList<Cluster> newLHS = new FastArrayList<>(LHS);
            FastArrayList<Cluster> newRHS = new FastArrayList<>(RHS);
            if (isLHS){
                newLHS = new FastArrayList<>(newSide);
            } else {
                newRHS = new FastArrayList<>(newSide);
            }
            int newSize = this.size / largest.size() * sc.size();
            ClusterCombination newCC = new ClusterCombination(newLHS, newRHS, this.level + 1, newSize);

            newCC.checkAndSetLB(this.getLB());
            newCC.checkAndSetUB(this.getUB());

            subCCs.add(newCC);

            // remove the subcluster to make room for the next subcluster
            newSide.remove(scPos);
        }
        return subCCs;
    }

//        Preserve descending order on id
    public boolean weightOverlapOneSide(Cluster newCluster, Cluster leftDupe, Cluster rightDupe){
        if (leftDupe != null && leftDupe.id < newCluster.id){
            return true;
        }
        if (rightDupe != null && rightDupe.id > newCluster.id){
            return true;
        }
        return false;
    }

//    Unpack CC to all cluster combinations with singleton clusters
    public FastLinkedList<ClusterCombination> getSingletons(double[] Wl, double[] Wr, boolean allowSideOverlap){
        FastLinkedList<ClusterCombination> out = new FastLinkedList<>();
        if (!this.isSingleton()) {
            FastArrayList<ClusterCombination> splitted = this.split(Wl, Wr, allowSideOverlap);
            for (ClusterCombination sc : splitted) {
                FastLinkedList<ClusterCombination> singletons = sc.getSingletons(Wl, Wr, allowSideOverlap);
                out.addAll(singletons);
            }
        }else{
            out.add(this);
        }
        return out;
    }

    public FastArrayList<ClusterCombination> unpackAndCheckConstraints(Parameters par){

        double[] Wl = par.Wl[this.getLHS().size() - 1];
        double[] Wr = par.maxPRight > 0 ? par.Wr[this.getRHS().size() - 1]: null;

        FastLinkedList<ClusterCombination> singletons = this.getSingletons(Wl, Wr, par.allowSideOverlap);
        FastArrayList<ClusterCombination> out = new FastArrayList<>(singletons.size());

        for (ClusterCombination cc: singletons){
            par.simMetric.bound(cc, Wl, Wr, par.pairwiseDistances);
            if (Math.abs(cc.getUB() - cc.getLB()) > 0.001) {
                par.getLOGGER().severe("postprocessing: found a singleton CC with LB != UB");
            }
            cc.setDecisive(true);

            double threshold = par.getRunningThreshold().get();

//                    Check if still positive
            boolean isResult = cc.getLB() > threshold;

//                    Check if not impacted by constraints
            if (isResult && LHS.size() + RHS.size() > 2 && (par.minJump > 0 || par.irreducibility)) {
                double subsetSim = cc.computeMaxSubsetSimilarity(par);

//                        All positive and negative dccs with minJump are constrained, negative dcc with irr is special constrained
//                        Check minJump
                if (par.minJump > 0) {
                    cc.setConstrained(true);
                    if (subsetSim + par.minJump > cc.getLB()){
                        isResult = false;
                    }
                }
//                        Check irreducibility
                if (par.irreducibility && subsetSim > threshold){
                    cc.setConstrained(true);
                    isResult = false;
                }
            }

            cc.setPositive(isResult);

            out.add(cc);
        }
        return out;
    }

    public ResultTuple toResultTuple(TimeSeries[] timeSeries){
//        Check if singleton, otherwise raise error
        FastArrayList<Integer> LHSIndices = new FastArrayList<>(LHS.stream().map(c -> c.pointsIdx.get(0)).collect(Collectors.toList()));
        FastArrayList<Integer> RHSIndices = new FastArrayList<>(RHS.stream().map(c -> c.pointsIdx.get(0)).collect(Collectors.toList()));

        if (this.isSingleton()){
            return new ResultTuple(
                    LHSIndices,
                    RHSIndices,
                    new FastArrayList<>(LHSIndices.stream().map(i -> timeSeries[i].getName()).collect(Collectors.toList())),
                    new FastArrayList<>(RHSIndices.stream().map(i -> timeSeries[i].getName()).collect(Collectors.toList())),
                    this.getLB(),
                    this.boundingTimestamp
            );
        } else {
            throw new IllegalArgumentException("Cluster combination is not a singleton");
        }

    }
}
