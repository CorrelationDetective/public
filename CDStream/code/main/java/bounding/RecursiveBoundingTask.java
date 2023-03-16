package bounding;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import clustering.Cluster;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import queries.ProgressiveStopException;
import queries.QueryTypeEnum;
import queries.ResultSet;
import streaming.index.DccIndex;
import streaming.index.ExtremaPair;
import streaming.index.IndexInstruction;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBoundingTask extends RecursiveAction {
    @NonNull private final ClusterCombination CC;
    @NonNull public double shrinkFactor;
    @NonNull public Parameters par;
    @NonNull public ResultSet resultSet;
    public static PriorityQueue<ClusterCombination> postponedDCCs =
            new PriorityQueue<>(10000, Comparator.comparingDouble(ClusterCombination::getCriticalShrinkFactor));

    public static void resetPostponedDCCs(){
        postponedDCCs = new PriorityQueue<>(10000, Comparator.comparingDouble(ClusterCombination::getCriticalShrinkFactor));
    }


    @Override
    protected void compute() {
        try {
            assessCC(CC);
        } catch (ProgressiveStopException e) {
            throw new RuntimeException(e);
        }

        if (!CC.isDecisive()) {
            FastArrayList<ClusterCombination> subCCs = CC.split(par.Wl[CC.getLHS().size() - 1], par.maxPRight > 0 ? par.Wr[CC.getRHS().size() - 1]: null,
                    par.allowSideOverlap);

//            If task is sufficiently small, run sequentially, otherwise fork
            if (par.parallel && CC.size() > 20) {
                par.statBag.addToStat(par.statBag.getNParallelCCs(), subCCs::size);
                ForkJoinTask.invokeAll(subCCs.stream().map(subCC -> new RecursiveBoundingTask(subCC, shrinkFactor, par, resultSet))
                        .collect(Collectors.toList()));
            } else {
                try {
                    sequentialCompute(new FastLinkedList<>(subCCs));
                } catch (ProgressiveStopException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void sequentialCompute(FastLinkedList<ClusterCombination> taskQueue) throws ProgressiveStopException {
//        Take tasks from the queue until it is empty
        while (!taskQueue.isEmpty()) {
            ClusterCombination canCC = taskQueue.poll();
            assessCC(canCC);
            par.statBag.incrementStat(par.statBag.getNSecCCs());

            if (!canCC.isDecisive()) {
                FastArrayList<ClusterCombination> subCCs = canCC.split(par.Wl[canCC.getLHS().size() - 1],
                        par.maxPRight > 0 ? par.Wr[canCC.getRHS().size() - 1]: null, par.allowSideOverlap);
                taskQueue.addAll(subCCs);
            }
        }
    }

//    Returned list is the list of new candidates -- in case this candidate is not decisive.
//    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD
    private void assessCC(ClusterCombination canCC) throws ProgressiveStopException {
        int p = canCC.getLHS().size() + canCC.getRHS().size();

        //        Compute/get bounds
        par.simMetric.bound(canCC,
                par.Wl[canCC.getLHS().size() - 1],
                par.maxPRight > 0 ? par.Wr[canCC.getRHS().size() - 1]: null,
                par.pairwiseDistances);

            //      Update statistics
        par.statBag.incrementStat(par.statBag.getNCCs());
        par.statBag.addToStat(par.statBag.getTotalCCSize(), canCC::size);

        double threshold = par.getRunningThreshold().get();

        //        Update threshold based on minJump and irreducibility if we have canCC > 2
        if (p > 2){
            if (par.minJump > 0 && canCC.getMaxPairwiseLB() + par.minJump > threshold) {
                threshold = Math.max(threshold, canCC.getMaxPairwiseLB() + par.minJump);
                canCC.setConstrained(true);
            }
            if (par.irreducibility && canCC.getMaxPairwiseLB() > threshold){
                threshold = Double.MAX_VALUE;
                canCC.setConstrained(true);
            }
        }

        //        Shrink upper bound for progressive bounding
        double shrunkUB = canCC.getShrunkUB(shrinkFactor, par.maxApproximationSize);

//        Check if canCC is indecisive
        if ((canCC.getLB() < threshold) && (shrunkUB > threshold)){
            canCC.setDecisive(false);
        }

        // canCC is decisive (or needs to be postponed)
        else {
            canCC.setDecisive(true);

//            CCs to be grouped if necessary
            FastArrayList<ClusterCombination> unpackedCCs = new FastArrayList<>(canCC.size());

//            Negative DCC, postpone for later if actual UB is above threshold (actually indecisive)
            if (shrunkUB < threshold) {
                if (canCC.getUB() > threshold) {
                    synchronized (postponedDCCs) {
                        canCC.setCriticalShrinkFactor(threshold);
                        postponedDCCs.add(canCC);
                    }
                    return; // don't group this
                }
                canCC.setPositive(false);

//                Index dcc
                if (par.useIndex()) indexDCC(canCC);

            } else if (canCC.getLB() > threshold){ //  Positive DCC
                canCC.setPositive(true);

                unpackedCCs = canCC.unpackAndCheckConstraints(par);

//                Add positives to resultset, index removed results if necessary
                unpackedCCs.stream().filter(ClusterCombination::isPositive)
                        .forEach(cc -> {
                            try {
                                ClusterCombination removedCC = resultSet.add(cc, par.getRunningThreshold());
//                                todo reindex
                            } catch (ProgressiveStopException e) {
                                throw new RuntimeException(e);
                            }
                        });

//                Index dccs if threshold query
                if (par.queryType.equals(QueryTypeEnum.THRESHOLD) && par.useIndex()){
                    for (ClusterCombination dcc: unpackedCCs){
                        indexDCC(dcc);
                    }
                }
            }
        }
    }

    public void indexDCC(ClusterCombination dcc){
//        Do not index artificial or dead dccs (e.g. from topK expansion)
        if (dcc.isArtificial() || dcc.isDead()) return;

//        Error handling
        if (!dcc.isDecisive()){
            par.LOGGER.severe("Trying to group indecisive DCC");
            return;
        }
        if (dcc.getLbIndexInstructions() == null || dcc.getUbIndexInstructions() == null){
            par.LOGGER.severe("Trying to group DCC without extrema pairs set for it");
            return;
        }

//        Cluster pairs structure we will be indexing the DCC on
        FastArrayList<IndexInstruction> indexInstructions = dcc.isPositive() ? dcc.getLbIndexInstructions() : dcc.getUbIndexInstructions();

//        Constrained DCCs need to be indexed on both bounds
//        (unless its a negative dcc constrained by irreducibility, they only need to be indexed on the lower bound)
        if (dcc.isConstrained()){
            if (!dcc.isPositive() && par.irreducibility){
                indexInstructions = dcc.getLbIndexInstructions();
            } else {
//                Create new list of index instructions of twice the size
                indexInstructions = new FastArrayList<>(indexInstructions.size() * 2);
                indexInstructions.addAll(dcc.getUbIndexInstructions());
                indexInstructions.addAll(dcc.getLbIndexInstructions());
            }
        }

//        Indexing -- follow the instructions
        for (IndexInstruction indexInstruction: indexInstructions){
            indexOnExtremaPair(dcc, indexInstruction);
        }

//        Clear index instructions after indexing
        dcc.clearIndexInstructions();

//        Update statistics
        par.statBag.incrementStat(par.statBag.getNIndexedDCCs());
        par.statBag.addToStat(par.statBag.getTotalIndexedDCCsSize(), dcc::size);
    }

    public void indexOnExtremaPair(ClusterCombination dcc, IndexInstruction indexInstruction){
        ClusterPair cp = indexInstruction.clusterPair;
        DccIndex dccIndex = indexInstruction.indexOnUb ? par.ubIndex : par.lbIndex;
        ExtremaPair ep = indexInstruction.indexOnUb ? cp.getUbDistExtrema() : cp.getLbDistExtrema();

//            Index on left cluster
        Cluster indexCluster = cp.c1;
        Cluster otherCluster = cp.c2;
        for (Integer i: indexCluster.pointsIdx){
//                index -> indexcolumn -> extremaGroup -> clusterGroup -> dccs
            dccIndex.indexColumns[i].getOrAdd(ep).getOrAdd(otherCluster).addDCC(dcc);
        }

//            Index on right cluster
        indexCluster = cp.c2;
        otherCluster = cp.c1;
        for (Integer i: indexCluster.pointsIdx){
//                index -> indexcolumn -> extremaGroup -> clusterGroup -> dccs
            dccIndex.indexColumns[i].getOrAdd(ep).getOrAdd(otherCluster).addDCC(dcc);
        }
    }
}
