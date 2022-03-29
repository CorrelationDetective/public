package bounding;

import _aux.PostProcessResults;
import clustering.*;

import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static _aux.lib.getStream;

public class AprioriPruning {


    int maxP;
    double minJump;
    double corrThreshold;
    boolean useEmpiricalBounds;
    HierarchicalBounding HB;
    public CorrelationBounding CB;
    ArrayList<ArrayList<ClusterCombination>> candidatesPerLevel;
    ArrayList<Cluster> allclusters;
    ArrayList<ArrayList<Cluster>> clustersPerLevel;
    int globalClustID;
//    public long n_positive;
    public long n_decided;
    boolean parallel;
    public List<ClusterCombination> positiveDCCs = new ArrayList<>(1000);
    public ArrayList<ClusterCombination> ApproximatedDCCs = new ArrayList<>(1000000);
    public List<String> positiveResultSet = new ArrayList<>(1000);
    public List<String> header;
    ProgressiveApproximation PA;
    Cluster rootCluster;



    public AprioriPruning(HierarchicalBounding HB, int maxP, double corrThreshold, boolean useEmpiricalBounds, double minJump, boolean parallelEvaluation, List<String> header){
        this.maxP=maxP;
        this.HB = HB;
        this.corrThreshold = corrThreshold;
        this.useEmpiricalBounds = useEmpiricalBounds;
        this.minJump = minJump;
//        this.candidatesPerLevel = new ArrayList<>();
        this.parallel = parallelEvaluation;
        this.header = header;




    }

    public void pruneWithApriori(double startEpsilon, double epsilonMultiplier, double shrinkFactor, double maxApproximationSize, int numBuckets, int topK, long startMS){

        // class containing all relevant tools for bounding cluster correlations -> put this at HB so that HierarchicalBounding can work with it:
        CB = new CorrelationBounding(HB.data, useEmpiricalBounds, parallel);
        HierarchicalBounding.CB = CB; // pass the corrbounding object to the hierarchbounding object to prevent double work

        // get clusters:
        long startClustering = System.currentTimeMillis();
        System.out.print("constructing hierarchical clustering...");
        RecursiveClustering RC = new RecursiveClustering(HB.data, CB, HB.maxLevels, HB.defaultDesiredClusters, HB.useKMeans, HB.breakFirstKLevelsToMoreClusters, HB.clusteringRetries);
        RC.fitRecursiveClusters(startEpsilon, epsilonMultiplier);
        long stopClustering = System.currentTimeMillis();
        System.out.println("   --> clustering finished, took " + ((stopClustering-startClustering)/1000) + " seconds");
        this.allclusters = RC.allClusters;
        this.clustersPerLevel = RC.clustersPerLevel;
        this.globalClustID = RC.globalClustID;

        CB.initializeBoundsCache(globalClustID, useEmpiricalBounds);

        ArrayList<Cluster> topLevelClusters = this.clustersPerLevel.get(0);
        Cluster rootCluster = topLevelClusters.get(0);
        this.rootCluster = rootCluster;



        // progressive approximation class in order to get results quickly:
        ProgressiveApproximation PA = new ProgressiveApproximation(corrThreshold, useEmpiricalBounds, minJump, maxApproximationSize, header, CB);



        //multipoles root candidate:
        ArrayList<Cluster> firstClusterComb = new ArrayList<>(2);
        firstClusterComb.add(rootCluster);// firstClusterComb.add(rootCluster);
//        List<ClusterCombination> candidates = new ArrayList<>(1);
//        ClusterCombination rootcandidate = new MultipoleClusterCombination(firstClusterComb);
//        candidates.add(rootcandidate);



        //multipearson root candidate:
//        ArrayList<Cluster> rootLHS = new ArrayList<>(1); rootLHS.add(rootCluster);
//        ArrayList<Cluster> rootRHS = new ArrayList<>(1); rootRHS.add(rootCluster); //rootRHS.add(rootCluster); rootRHS.add(rootCluster);
//        ClusterCombination rootcandidate = new MultiPearsonClusterCombination(rootLHS, rootRHS);




        //progress overview:
        System.out.print("|");
        for(int i = 0; i<100; i++){
            System.out.print(".");
        }
        System.out.print("|");
        System.out.println();


        // level-wise evaluation of the candidates
        for(int p=2; p<=maxP; p++){
            firstClusterComb.add(rootCluster);
            ClusterCombination rootcandidate = new MultipoleClusterCombination(firstClusterComb);

            List<ClusterCombination> candidates = new ArrayList<>(1);
            candidates.add(rootcandidate);

            System.out.println("---------------start evaluating level: "+ p + " at time " + LocalTime.now() +". Number of positives so far: " + this.positiveResultSet.size() +". Runtime so far: " + (System.currentTimeMillis() - startMS)/1000);

            Map<Boolean, List<ClusterCombination>> groupedDCCs = evaluateCandidates(candidates, shrinkFactor, maxApproximationSize, parallel);

            if(positiveDCCs.size() > topK){
                positiveDCCs = getStream(positiveDCCs, parallel)
                        .sorted((cc1, cc2) -> Double.compare(cc2.getLB(), cc1.getLB()))
                        .limit(topK)
                        .collect(Collectors.toList());
                PA.corrThreshold = positiveDCCs.get(positiveDCCs.size()-1).getLB();
                corrThreshold = PA.corrThreshold;
            }


            //progressive approximation within this level p:
            List<ClusterCombination> approximatedCCs = getStream(groupedDCCs.get(false), parallel).filter(dcc -> dcc.getCriticalShrinkFactor() <= 1).collect(Collectors.toList());
//            PA.testApproximationQuality(approximatedCCs, numBuckets, p, parallel);
            this.positiveDCCs = PA.ApproximateProgressively(approximatedCCs, numBuckets, positiveDCCs, parallel, topK, startMS);
            corrThreshold = PA.corrThreshold; // for topK -> carry over the updated tau to next p
            this.positiveResultSet = PostProcessResults.removeDuplicatesAndToString(this.positiveDCCs, header, CB, parallel);

            // get the approximated candidates to generate the next-level candidates from. the advantage is that we get a head-start down the clustering tree and do not have to start from the top
            ArrayList<ClusterCombination> DCCs = new ArrayList<>(groupedDCCs.get(false).size() + groupedDCCs.get(true).size());
            DCCs.addAll(groupedDCCs.get(false)); DCCs.addAll(groupedDCCs.get(true));
            System.out.println("tau updated to " + corrThreshold);



//            if(p<maxP){
//                System.out.println("level " + p + " finished at time "+ LocalTime.now() + ". generating candidates for level " + (p+1) +". #DCCs: " + DCCs.size() +". positives so far: " + this.positiveResultSet.size() + ". Runtime so far (s): " + (System.currentTimeMillis()-startMS)/1000);
//
//
//
//                if(DCCs.get(0).isMultipoleCandidate()){ // multipole pattern
//
//                    candidates = generateCandidates(DCCs, parallel);
//
//                }else{ // multiPearson pattern -> make distinction on adding vectors to LHS or RHS. better performance: use HIerarchicalbounding without level generation.
//
//
//                    candidates = new ArrayList<>();
//
//
//
//                    // first generate candidates by adding to RHS
//                    if(parallel){
//                        DCCs = (ArrayList<ClusterCombination>) DCCs.parallelStream()
//                                .sorted(this::lexicographicalOrdering)
//                                .collect(Collectors.toList());
//                    }else{
//                        DCCs.sort(this::lexicographicalOrdering);
//                    }
//                    List<ClusterCombination> partial = generateCandidates(DCCs, parallel);
//                    if(partial!= null){
//                        candidates.addAll(partial);
//                    }
//
//                    //now by adding to LHS: (hack by swapping the LHS and RHS and calling the same methods
//                    //note that we do not need to swap those in which the size of the LHS and RHS is the same
//                    ArrayList<ClusterCombination> swappedDCCs;
//                    if(parallel){
//                        swappedDCCs = (ArrayList<ClusterCombination>) DCCs.parallelStream()
//                                .filter(cc -> cc.getLHS().size() != cc.getRHS().size()) // ignore those where lhs and rhs are of the same size
//                                .collect(Collectors.toList());
//                        swappedDCCs.parallelStream()
//                                .forEach(cc -> ((MultiPearsonClusterCombination) cc).swapLeftRightSide());
//
//                        swappedDCCs = (ArrayList<ClusterCombination>) swappedDCCs.parallelStream()
//                                .sorted(this::lexicographicalOrdering)
//                                .collect(Collectors.toList());
//                    }else{
//                        swappedDCCs = (ArrayList<ClusterCombination>) DCCs.stream()
//                                .filter(cc -> cc.getLHS().size() != cc.getRHS().size()) // ignore those where lhs and rhs are of the same size
//                                .collect(Collectors.toList());
//
//                        swappedDCCs.forEach(cc -> ((MultiPearsonClusterCombination) cc).swapLeftRightSide());
//                        swappedDCCs.sort(this::lexicographicalOrdering);
//                    }
//
//                    partial = generateCandidates(swappedDCCs, parallel);
//                    if(partial!= null){
//                        candidates.addAll(partial);
//                    }
//
//
//                }
//
//
//            }
        }

    }


    public Map<Boolean, List<ClusterCombination>> evaluateCandidates(List<ClusterCombination> candidates, double shrinkFactor, double maxApproximationSize, boolean parallel){
        // also returns the negative DCCs for next-level candidate generation

        //split the DCCs into positve and negative ones
        Map<Boolean, List<ClusterCombination>> DCCs = getStream(candidates, parallel).unordered()
                .flatMap(CC -> HierarchicalBounding.corrBounding(CC, corrThreshold, useEmpiricalBounds, parallel, minJump, shrinkFactor, maxApproximationSize).stream())
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));


        this.positiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(DCCs.get(true), CB, minJump, parallel));
        this.positiveResultSet = PostProcessResults.removeDuplicatesAndToString(this.positiveDCCs, header, CB, parallel);

        return DCCs;
    }



    public List<ClusterCombination> generateCandidates(ArrayList<ClusterCombination> oldLevelCandidates, boolean parallel){

        ArrayList<ClusterCombination> candidatesThatMaySatisfyMinjump = (ArrayList<ClusterCombination>) getStream(oldLevelCandidates, parallel)
                .unordered()
                .filter( cc -> (1 - cc.getLB()) > minJump)
                .sorted(this::lexicographicalOrdering)
                .collect(Collectors.toList());
//        ArrayList<ClusterCombination> candidatesThatMaySatisfyMinjump = oldLevelCandidates;

        List<ClusterCombination> readOnlyCandidates = Collections.unmodifiableList(candidatesThatMaySatisfyMinjump);

        int size = readOnlyCandidates.size();

        IntStream ints = IntStream.range(0, size);
        if(parallel){
            ints = ints.parallel();
        }

        List<ClusterCombination> out = ints
                .unordered()
                .mapToObj(i -> getCandidatesForLHS(readOnlyCandidates, i))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        //return object of new candidates:

        return out;

    }





    ClusterCombination checkAndDoJoin(ClusterCombination CC1, ClusterCombination CC2){

        if(CC1.isMultipoleCandidate()){
            return checkAndDoJoinMultipoles(CC1, CC2);
        }else{
            return checkAndDoJoinMultiPearson(CC1, CC2);
        }

    }

    private ClusterCombination checkAndDoJoinMultiPearson(ClusterCombination CC1, ClusterCombination CC2) {


        // let's make the convention that a cluster is always added to the RHS. If we want to add to LHS, we swap the RHS and LHS within the candidate

//        System.err.println("multiPearson does not yet support canidate generation");
        boolean doJoin = true;
        ArrayList<Cluster> CCList = new ArrayList<>(CC1.getClusters().size()+1); //list of output clusters for new CC if join is succesfull

        ArrayList<Cluster> CC1Clusters = CC1.getClusters();
        ArrayList<Cluster> CC2Clusters = CC2.getClusters();

        ArrayList<Cluster> LHS1 = CC1.getLHS();
        ArrayList<Cluster> LHS2 = CC2.getLHS();

        ArrayList<Cluster> RHS1 = CC1.getRHS();
        ArrayList<Cluster> RHS2 = CC2.getRHS();

        for(int i =0; i<CC1Clusters.size()-1; i++) {
            Cluster C1 = CC1Clusters.get(i);
            Cluster C2 = CC2Clusters.get(i);

            boolean overlap = checkClusterOverlap(C1, C2); // check if there is some overlap



            if(!overlap){ //the cluster in this position does not overlap, we don't need to join
                doJoin=false;
                break;
            }else{ // there is overlap, add the intersection of C1 and C2
                if(C1.getNPoints() <= C2.getNPoints()){
                    CCList.add(C1);
                }else{
                    CCList.add(C2);
                }

            }


        }


        if(doJoin){ // each cluster in the candidate (except the last) does overlap
            // so, according to apriori, we join -> return the overlapping clusters + the last clusters of CC1 and CC2
            Cluster lastCC1 = CC1Clusters.get(CC1Clusters.size()-1);
            Cluster lastCC2 = CC2Clusters.get(CC2Clusters.size()-1);
            if(lastCC1.getClusterId() <= lastCC2.getClusterId()){ // make sure to respect ordering of clusters within a candidate by ID. otherwise this could be designated as a duplicate
                CCList.add(lastCC1); CCList.add(lastCC2);
            }else{
                CCList.add(lastCC2); CCList.add(lastCC1);
            }

            ArrayList<Cluster> newLHS = new ArrayList<>();
            ArrayList<Cluster> newRHS = new ArrayList<>();
            for(int i = 0; i<LHS1.size(); i++){
                newLHS.add(CCList.get(i));
            }
            for(int i = LHS1.size(); i<CCList.size(); i++){
                newRHS.add(CCList.get(i));
            }

            ClusterCombination newCC = new MultiPearsonClusterCombination(newLHS, newRHS);
            newCC.checkAndSetMaxSubsetLowerBound(Math.max(CC1.getLB(), CC1.getMaxLowerBoundSubset()));
            newCC.checkAndSetMaxSubsetLowerBound(Math.max(CC2.getLB(), CC2.getMaxLowerBoundSubset()));

            return newCC;

        }else{
            return null;
        }
    }

    private ClusterCombination checkAndDoJoinMultipoles(ClusterCombination CC1, ClusterCombination CC2) {
        boolean doJoin = true;
        ArrayList<Cluster> CCList = new ArrayList<>(CC1.getClusters().size()+1); //list of output clusters for new CC if join is succesfull

        ArrayList<Cluster> CC1Clusters = CC1.getClusters();
        ArrayList<Cluster> CC2Clusters = CC2.getClusters();

        for(int i =0; i<CC1Clusters.size()-1; i++) {
            Cluster C1 = CC1Clusters.get(i);
            Cluster C2 = CC2Clusters.get(i);

            boolean overlap = checkClusterOverlap(C1, C2); // check if there is some overlap



            if(!overlap){ //the cluster in this position does not overlap, we don't need to join
                doJoin=false;
                break;
            }else{ // there is overlap, add the intersection of C1 and C2
                if(C1.getNPoints() <= C2.getNPoints()){
                    CCList.add(C1);
                }else{
                    CCList.add(C2);
                }

            }


        }


        if(doJoin){ // each cluster in the candidate (except the last) does overlap
            // so, according to apriori, we join -> return the overlapping clusters + the last clusters of CC1 and CC2
            Cluster lastCC1 = CC1Clusters.get(CC1Clusters.size()-1);
            Cluster lastCC2 = CC2Clusters.get(CC2Clusters.size()-1);
            if(lastCC1.getClusterId() <= lastCC2.getClusterId()){ // make sure to respect ordering of clusters within a candidate by ID. otherwise this could be marked as a duplicate
                CCList.add(lastCC1); CCList.add(lastCC2);
            }else{
                CCList.add(lastCC2); CCList.add(lastCC1);
            }

            ClusterCombination newCC = new MultipoleClusterCombination(CCList);
            newCC.checkAndSetMaxSubsetLowerBound(CC1.getLB());
            newCC.checkAndSetMaxSubsetLowerBound(CC2.getLB());

            return newCC;

        }else{
            return null;
        }




    }


    boolean checkClustersOverlapNaive(Cluster C1, Cluster C2){
        Cluster left; Cluster right;
        if (C1.getNPoints() <= C2.getNPoints()) {
            left = C1;
            right = C2;
        } else {
            left = C2;
            right = C1;
        }

        //check if clusters overlap: all points in left (the smaller cluster) must then be in right as only subclusters can overlap
        int pLeft = left.listOfContents.get(0);
        boolean overlap = false;
        for (int pRight : right.listOfContents) {
            if (pLeft == pRight) {
                overlap = true;
                break;
            }
        }

        return overlap;
    }

    boolean checkClusterOverlap(Cluster C1, Cluster C2){
        int CID1 = C1.getClusterId(); int CID2 = C2.getClusterId();

        if(CID1 <= CID2){ // only way for overlap is if c1 is a supercluster of c2, or it is c2
            return CID2 <= C1.getLargestSubClusterID();
        }else{ //only way for overlap is if c2 is a supercluster of c1
            return CID1 <= C2.getLargestSubClusterID();
        }
    }


    private int lexicographicalOrdering(ClusterCombination CC1, ClusterCombination CC2){
        ArrayList<Cluster> clusters1 = CC1.getClusters();
        ArrayList<Cluster> clusters2 = CC2.getClusters();

        for (int i = 0; i < clusters1.size(); i++) {
            Cluster c1 = clusters1.get(i);
            Cluster c2 = clusters2.get(i);
            if (c1 != c2) {
                return c1.getClusterId() - c2.getClusterId();
            }
        }
        return 0;
    };


//    private Map<Boolean, List<ClusterCombination>> evaluateCandidatesParallel(List<ClusterCombination> candidates, double shrinkFactor, double maxApproximationSize) {
//        //parallel:
//
//
//        //split the DCCs into positve and negative ones
//        Map<Boolean, List<ClusterCombination>> DCCs = candidates.parallelStream().unordered()
//                .flatMap(CC -> HierarchicalBounding.corrBounding(CC, corrThreshold, useEmpiricalBounds, true, minJump, shrinkFactor, maxApproximationSize).stream())
//                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
//
//
//        this.postiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(DCCs.get(true), CB, minJump));
//        this.positiveResultSet = PostProcessResults.removeDuplicatesAndToString(this.postiveDCCs, header, CB);
//
//        return DCCs;
//    }

//    private Map<Boolean, List<ClusterCombination>> evaluateCandidatesSequential(List<ClusterCombination> candidates, double shrinkFactor, double maxApproximationSize) {
//        // sequential:
//        Map<Boolean, List<ClusterCombination>> DCCs = candidates.stream().unordered()
//                .flatMap(CC -> HierarchicalBounding.corrBounding(CC, corrThreshold, useEmpiricalBounds, false, minJump, shrinkFactor, maxApproximationSize).stream())
//                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
//
//        this.postiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(DCCs.get(true), CB, minJump));
//        this.positiveResultSet = PostProcessResults.removeDuplicatesAndToString(this.postiveDCCs, header, CB);
//
//        return DCCs;
//    }

//    private ArrayList<ClusterCombination> generateCandidatesSequential(ArrayList<ClusterCombination> oldLevelCandidates) {
//
//        List<ClusterCombination> candidatesThatMaySatisfyMinjump =  oldLevelCandidates
//                .stream().unordered()
//                .filter( cc -> (1 - cc.getLB()) > minJump)
//                .sorted(this::lexicographicalOrdering)
//                .collect(Collectors.toList());
//
//
//        int size = candidatesThatMaySatisfyMinjump.size();
//
//
//        List<ClusterCombination> out = IntStream.range(0, size).unordered()
//                .mapToObj(i -> getCandidatesForLHS(candidatesThatMaySatisfyMinjump, i))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//
////        List<ClusterCombination> out = candidatesThatMaySatisfyMinjump.stream().unordered().map(cc ->{
////            ArrayList<Cluster> clusters = cc.getClusters();
////            clusters.add(rootCluster);
////            return (ClusterCombination) new MultipoleClusterCombination(clusters);
////        }).collect(Collectors.toList());
//
//
//        return (ArrayList<ClusterCombination>) out;
//    }

//    private List<ClusterCombination> generateCadidatesParallel(ArrayList<ClusterCombination> oldLevelCandidates) {
//
//        ArrayList<ClusterCombination> candidatesThatMaySatisfyMinjump = (ArrayList<ClusterCombination>) oldLevelCandidates
//                .parallelStream().unordered()
//                .filter( cc -> (1 - cc.getLB()) > minJump)
//                .sorted(this::lexicographicalOrdering)
//                .collect(Collectors.toList());
////        ArrayList<ClusterCombination> candidatesThatMaySatisfyMinjump = oldLevelCandidates;
//
//        List<ClusterCombination> readOnlyCandidates = Collections.unmodifiableList(candidatesThatMaySatisfyMinjump);
//
//        int size = readOnlyCandidates.size();
//
//        List<ClusterCombination> out = IntStream.range(0, size)
//                .parallel().unordered()
//                .mapToObj(i -> getCandidatesForLHS(readOnlyCandidates, i))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//
////        List<ClusterCombination> out = candidatesThatMaySatisfyMinjump.stream().unordered().map(cc ->{
////            ArrayList<Cluster> clusters = cc.getClusters();
////            clusters.add(rootCluster);
////            return (ClusterCombination) new MultipoleClusterCombination(clusters);
////        }).collect(Collectors.toList());
//
//        return out;
//
//
//
//    }

    private ArrayList<ClusterCombination> getCandidatesForLHS(List<ClusterCombination> sortedCC, int i){
        int size = sortedCC.size();
        ClusterCombination CC1 = sortedCC.get(i);
        ArrayList<ClusterCombination> out = new ArrayList<>(30);

        int j=i;
        while(j<size){
            ClusterCombination CC2 = sortedCC.get(j);

            ClusterCombination joined = checkAndDoJoin(CC1, CC2);

            if(joined != null) {
                out.add(joined);
                j++;
            }else {//     if first clusters do not overlap, break, as all CCs later on will not overlap either (the first clusters can never overlap)
                j = getNextj(sortedCC, i, j);
            }
        }

        return out;
    }

    private int getNextj(List<ClusterCombination> sortedCC, int i, int j){
        int out = j;

        ArrayList<Cluster> cleft = sortedCC.get(i).getClusters();
        ArrayList<Cluster> cright = sortedCC.get(j).getClusters();

        int pos = getPosNonOverlap(cleft, cright); // first non overlapping cluster

        if(pos==0){
            return sortedCC.size(); // done for this i, terminate loop
        }

        Cluster left = cleft.get(pos);
        Cluster right = cright.get(pos);

        if(right.getClusterId() <= left.getClusterId()){ // there might be super clusters of left down the line, but we can skip all subclusters of right (skipall=false)
            out = lexicoSearch(sortedCC, j, pos, false);
        }else{ // right CID > left CID and non-overlapping, then we can find the next CID of one position earlier (skipall=true)
            out = lexicoSearch(sortedCC, j, pos, true);
        }


        return out;
    }

    private int lexicoSearch(List<ClusterCombination> sortedCC, int j, int posNonOverlap, boolean skipall){

        ClusterCombination CCRight = sortedCC.get(j);
        ArrayList<Cluster> imagClusters = new ArrayList<>(CCRight.getClusters().size());
        int pos;
        int newCIDAtPos;
        if(skipall){ // cid of right is larger than left -> go back one level and find the next possible candidate
            pos = posNonOverlap - 1; // this is the position to check. note posnonoverlap >= 1
            while(CCRight.getClusters().get(pos).getClusterId() >= allclusters.size()-1 && pos>0){
                pos = pos-1;
            }
            newCIDAtPos = CCRight.getClusters().get(pos).getClusterId() + 1; // get the cluster with id old CID + 1
            if(pos == 0 && newCIDAtPos >= allclusters.size()){ // we can never find this cluster because it does not exist
                return sortedCC.size();
            }
        }else{ //cid of right is smaller than left, but no overlap. so we can skip all subclusters of right
            pos = posNonOverlap;
            newCIDAtPos = CCRight.getClusters().get(pos).getLargestSubClusterID()+1;
        }


        for(int p = 0; p<pos; p++){
            imagClusters.add(CCRight.getClusters().get(p));
        }

        if(newCIDAtPos >= allclusters.size()){
            System.err.println("this goes wrong");
        }

        imagClusters.add(allclusters.get(newCIDAtPos));  //todo: can throw error: this index is > than the number of clusters. in that case we need to go one pos back?

        for(int p = pos+1; p<CCRight.getClusters().size(); p++){
            imagClusters.add(clustersPerLevel.get(0).get(0)); //root -> lowest index
        }


        ClusterCombination imaginary = new MultipoleClusterCombination(imagClusters);

        int id = this.binarySearchImag(sortedCC, imaginary, this::lexicographicalOrdering); // index if imaginary exists, otherwise a negative number

        return id;
    }

    private int getPosNonOverlap(ArrayList<Cluster> left, ArrayList<Cluster> right){

        for(int i=0; i < left.size()-1; i++){
            if(!checkClusterOverlap(left.get(i), right.get(i))){
                return i;
            }
        }
        System.err.println("AP: trying to find non-overlapping pos but not found");
        return -1; // should raise exception
    }


    private int binarySearchImag(List<ClusterCombination> sortedCC, ClusterCombination cc, Comparator<ClusterCombination> comp){
        int id = Collections.binarySearch(sortedCC, cc, comp);

        if(id < 0){ // cc does not exist in sortedCC -> return next higher id. see docs
            //binarysearch will return -1-i where i is the index of the next higher element in the list -> we want this i
            return -1 - id;
        }else{ //found, very unlikely
            return id;
        }
    }

}


