package algorithms.baselines;

import _aux.Pair;
import _aux.lists.FastArrayList;
import bounding.ClusterCombination;
import core.Parameters;
import clustering.Cluster;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class SmartBaseline extends Baseline{
    Cluster[] singletonClusters;

    public SmartBaseline(Parameters par) {
        super(par);
    }

    @Override
    public void prepare(){
        par.setPairwiseDistances(par.simMetric.computePairwiseDistances(par.data, true));
        makeSingletonClusters();
    }

    private void makeSingletonClusters(){
        singletonClusters = new Cluster[par.n];
        for(int i = 0; i < par.n; i++){
            Cluster c = new Cluster(par.simMetric.distFunc, i, par.geoCentroid);
            c.setId(i);
            c.finalize(par.data, par.pairwiseDistances);
            singletonClusters[i] = c;
        }
        par.simMetric.setTotalClusters(par.n);
    }

//    Compute similarities based on pairwise similarities (if possible)
    public double computeSimilarity(Pair<FastArrayList<Integer>, FastArrayList<Integer>> candidate){
//        Create cluster combination
        FastArrayList<Cluster> LHS = new FastArrayList<>(candidate.x.stream().map(i -> singletonClusters[i]).collect(Collectors.toList()));
        FastArrayList<Cluster> RHS = new FastArrayList<>(candidate.y.stream().map(i -> singletonClusters[i]).collect(Collectors.toList()));
        ClusterCombination cc = new ClusterCombination(LHS, RHS, 0, candidate.x.size() + candidate.y.size());
        par.simMetric.bound(cc, par.empiricalBounding,
                par.Wl[candidate.x.size() - 1], par.maxPRight > 0 ? par.Wr[candidate.y.size() - 1] : null,
                par.pairwiseDistances, par.statBag.stopWatch.getNanoTime());
        return cc.getLB();
    }
}
