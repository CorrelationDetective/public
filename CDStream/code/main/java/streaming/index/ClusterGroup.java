package streaming.index;

import bounding.ClusterCombination;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ClusterGroup {
    @NonNull public Cluster cluster;
    @Getter private ArrayList<ClusterCombination> positiveDCCs = new ArrayList<>(10);
    @Getter private ArrayList<ClusterCombination> negativeDCCs = new ArrayList<>(10);

    public void addDCC(ClusterCombination CC){
        if (CC.isPositive()){
            synchronized (positiveDCCs){
                positiveDCCs.add(CC);
            }
        } else {
            synchronized (negativeDCCs){
                negativeDCCs.add(CC);
            }
        }
    }

    public void filterOutDeadDCCs(){
        positiveDCCs = positiveDCCs.stream()
                .filter(cc -> !cc.isDead() && cc.isDecisive() && cc.isPositive())
                .collect(Collectors.toCollection(ArrayList::new));
        negativeDCCs = negativeDCCs.stream()
                .filter(cc -> !cc.isDead() && cc.isDecisive() && !cc.isPositive())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public String toString(){
        return Integer.toString(cluster.getId());
    }

    public int size(){
        return positiveDCCs.size() + negativeDCCs.size();
    }
}
