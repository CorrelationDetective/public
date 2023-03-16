package algorithms.statics.baselines;

import _aux.Pair;
import _aux.lists.FastArrayList;
import bounding.ClusterCombination;
import core.Parameters;
import clustering.Cluster;

import java.util.stream.Collectors;

public class SmartBaseline extends Baseline{
    Cluster[] singletonClusters;

    public SmartBaseline(Parameters par) {
        super(par);
    }

    @Override
    public void prepare(){
        par.setPairwiseDistances(par.simMetric.computePairwiseDistances(par.timeSeries, true));
        makeSingletonClusters();
    }

    private void makeSingletonClusters(){
        singletonClusters = new Cluster[par.n];
        for(int i = 0; i < par.n; i++){
            Cluster c = new Cluster(par.simMetric.distFunc, i);
            c.setId(i);
            c.finalize(par.timeSeries, par.pairwiseDistances);
            singletonClusters[i] = c;
        }
        par.simMetric.setTotalClusters(par.n);
    }

//    Compute similarities based on pairwise similarities (if possible)
    public ClusterCombination computeSimilarity(Pair<FastArrayList<Integer>, FastArrayList<Integer>> candidate){
//        Create cluster combination
        FastArrayList<Cluster> LHS = new FastArrayList<>(candidate._1.stream().map(i -> singletonClusters[i]).collect(Collectors.toList()));
        FastArrayList<Cluster> RHS = new FastArrayList<>(candidate._2.stream().map(i -> singletonClusters[i]).collect(Collectors.toList()));
        ClusterCombination cc = new ClusterCombination(LHS, RHS, 0, candidate._1.size() * candidate._2.size());
        par.simMetric.bound(cc, par.Wl[candidate._1.size() - 1], par.maxPRight > 0 ? par.Wr[candidate._2.size() - 1] : null,
                par.pairwiseDistances);
        return cc;
    }
}
