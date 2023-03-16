package algorithms.baselines;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastArrayList;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ConcurrentHashMap;

public class SimpleBaseline extends Baseline{
    @RequiredArgsConstructor
    private class SimCacheValue{
        @NonNull public FastArrayList<Integer> left;
        @NonNull public FastArrayList<Integer> right;
        @NonNull public double sim;
    }

    ConcurrentHashMap<Long, SimCacheValue> simCache = new ConcurrentHashMap<>();

    public SimpleBaseline(Parameters par) {
        super(par);
    }

    @Override public void prepare(){}

//    Compute similarities exhaustively
    public double computeSimilarity(Pair<FastArrayList<Integer>, FastArrayList<Integer>> candidate){
        long hash = hashCandidate(candidate.x, candidate.y);

        if (simCache.containsKey(hash)){
            SimCacheValue val = simCache.get(hash);
            if (val.left.equals(candidate.x) && val.right.equals(candidate.y)){
                return val.sim;
            }
        }
        double[] v1 = par.simMetric.preprocess(linearCombination(candidate.x, WlFull));
        double[] v2 = par.simMetric.preprocess(linearCombination(candidate.y, WrFull));
        double sim = par.simMetric.sim(v1,v2);
        simCache.put(hash, new SimCacheValue(candidate.x, candidate.y, sim));
        return sim;
    }

    public long hashCandidate(FastArrayList<Integer> left, FastArrayList<Integer> right){
        FastArrayList<Integer> hashList = new FastArrayList<>(left.size() + right.size() + 1);
        hashList.addAll(left);
        hashList.add(-1);
        hashList.addAll(right);
        return hashList.hashCode();
    }
}
