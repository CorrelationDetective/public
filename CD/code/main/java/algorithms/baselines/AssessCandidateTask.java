package algorithms.baselines;

import _aux.Pair;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import queries.ProgressiveStopException;
import queries.ResultObject;
import queries.ResultSet;
import queries.ResultTuple;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.function.Function;

@RequiredArgsConstructor
public class AssessCandidateTask extends RecursiveAction {
    @NonNull FastArrayList<Integer> LHS;
    @NonNull FastArrayList<Integer> RHS;
    @NonNull double maxSubsetSimilarity;
    @NonNull Function<Pair<FastArrayList<Integer>, FastArrayList<Integer>>, Double> similarityComputer;

    public static Parameters par;
    public static ResultSet resultSet;

    @Override
    public void compute(){
//        Assess this candidate
        boolean isResult;
        try {
            isResult = assessCandidate(LHS,RHS);
        } catch (ProgressiveStopException e){
            throw new RuntimeException(e);
        }

        int leftSize = LHS.size();
        int rightSize = RHS.size();
        int p = leftSize + rightSize;

//        Expand candidate if necessary
//        Expand candidate if not at max P and necessary (considering potential irreducibility constraint)
        if (p < par.maxPLeft + par.maxPRight && !(par.irreducibility && isResult)){
            // Expand candidate
            boolean expandLeft = rightSize == par.maxPRight || (leftSize == rightSize && leftSize < par.maxPLeft);

            // Expand left
            FastLinkedList<AssessCandidateTask> newTasks = new FastLinkedList<>();
            if (expandLeft){
                for (int i = LHS.get(leftSize-1) + 1; i < par.n; i++) {
                    if (RHS.contains(i)) continue;

                    FastArrayList<Integer> newLHS = new FastArrayList<>(LHS.size() + 1);
                    newLHS.addAll(LHS);
                    newLHS.add(i);
                    newTasks.add(new AssessCandidateTask(newLHS, RHS, maxSubsetSimilarity, similarityComputer));
                }
            } else {
                for (int i = RHS.get(rightSize-1) + 1; i < par.n; i++) {
                    if (LHS.contains(i)) continue;

                    FastArrayList<Integer> newRHS = new FastArrayList<>(RHS.size() + 1);
                    newRHS.addAll(RHS);
                    newRHS.add(i);
                    newTasks.add(new AssessCandidateTask(LHS, newRHS, maxSubsetSimilarity, similarityComputer));
                }
            }

//            Invoke all new tasks
            ForkJoinTask.invokeAll(newTasks.toList());
        }
    }

    public boolean assessCandidate(FastArrayList<Integer> LHS, FastArrayList<Integer> RHS) throws ProgressiveStopException {
        // Compute similarity of this candidate
        double sim = similarityComputer.apply(new Pair<>(LHS, RHS));

        // Add candidate to result dependent of similarity and query parameters
        double threshold = par.tau;

        //  Increase for minJump
        if (LHS.size() + RHS.size() > 2){
            double jumpBasedThreshold = maxSubsetSimilarity + par.minJump;
            threshold = Math.max(threshold, jumpBasedThreshold);
        }
        maxSubsetSimilarity = Math.max(maxSubsetSimilarity, sim);

        //        Check if above threshold, if so add to results
        boolean isResult = sim >= threshold;
        if (isResult) {
            synchronized (resultSet){
                resultSet.add(new ResultTuple(LHS, RHS, new FastArrayList<>(0), new FastArrayList<>(0), sim, System.nanoTime()),
                        par.runningThreshold);
            }
        }
        return isResult;
    }
}
