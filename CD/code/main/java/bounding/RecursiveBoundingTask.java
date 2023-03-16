package bounding;

import _aux.lists.FastLinkedList;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import queries.ProgressiveStopException;
import queries.QueryTypeEnum;
import queries.ResultSet;
import similarities.functions.TotalCorrelation;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBoundingTask extends RecursiveAction {
    @NonNull private final ClusterCombination CC;
    public static Parameters par;
    public static double shrinkFactor = 1;
    public static ResultSet resultSet;
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
            FastLinkedList<ClusterCombination> subCCs = CC.split(par.Wl[CC.getLHS().size() - 1], par.maxPRight > 0 ? par.Wr[CC.getRHS().size() - 1]: null,
                    par.allowSideOverlap);

//            If task is sufficiently small, run sequentially, otherwise fork
            if (par.parallel && CC.size() > 20) {
                par.statBag.addStat(par.statBag.getNParallelCCs(), subCCs::size);
                ForkJoinTask.invokeAll(subCCs.stream().map(RecursiveBoundingTask::new).collect(Collectors.toList()));
            } else {
                try {
                    sequentialCompute(new FastLinkedList<>(subCCs));
                } catch (ProgressiveStopException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static void sequentialCompute(FastLinkedList<ClusterCombination> taskQueue) throws ProgressiveStopException {
//        Take tasks from the queue until it is empty
        while (!taskQueue.isEmpty()) {
            ClusterCombination canCC = taskQueue.poll();
            assessCC(canCC);
            par.statBag.incrementStat(par.statBag.getNSecCCs());

            if (!canCC.isDecisive()) {
                FastLinkedList<ClusterCombination> subCCs = canCC.split(par.Wl[canCC.getLHS().size() - 1],
                        par.maxPRight > 0 ? par.Wr[canCC.getRHS().size() - 1]: null, par.allowSideOverlap);
                taskQueue.addAll(subCCs);
            }
        }
    }

//    Returned list is the list of new candidates -- in case this candidate is not decisive.
//    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD
    private static void assessCC(ClusterCombination canCC) throws ProgressiveStopException {
        int p = canCC.getLHS().size() + canCC.getRHS().size();

        //        Compute/get bounds
        par.simMetric.bound(canCC, par.empiricalBounding,
                par.Wl[canCC.getLHS().size() - 1], par.maxPRight > 0 ? par.Wr[canCC.getRHS().size() - 1]: null,
                par.pairwiseDistances, par.statBag.stopWatch.getNanoTime());

        //      Update statistics
        par.statBag.incrementStat(par.statBag.getNCCs());
        par.statBag.addStat(par.statBag.getTotalCCSize(), canCC::size);

        double threshold = par.runningThreshold.get();

        //        Update threshold based on minJump and irreducibility if we have canCC > 2
        if (p > 2){
            if (par.minJump > 0){
                threshold = Math.max(threshold, canCC.getMaxPairwiseLB() + par.minJump);
            }
            if (par.irreducibility && canCC.getMaxPairwiseLB() >= threshold){
                threshold = Double.MAX_VALUE;
            }
        }

        //        Shrink upper bound for progressive bounding
        double shrunkUB = canCC.getShrunkUB(shrinkFactor, par.maxApproximationSize);

//        Check if canCC is (in)decisive
        if ((canCC.getLB() < threshold) && (shrunkUB > threshold)){
            canCC.setDecisive(false);
        } else { // canCC is decisive, add to DCCs
            canCC.setDecisive(true);

//            Negative DCC, postpone for later if actual UB is above threshold (actually indecisive)
            if (shrunkUB < threshold) {
                if (canCC.getUB() > threshold) {
                    synchronized (postponedDCCs) {
                        canCC.setCriticalShrinkFactor(threshold);
                        postponedDCCs.add(canCC);
                    }
                }
            } else if (canCC.getLB() > threshold){ //  Positive DCC
                canCC.setPositive(true);
                resultSet.addAll(canCC.unpackAndCheckConstraints(par), par.runningThreshold);
            }
        }
    }
}
