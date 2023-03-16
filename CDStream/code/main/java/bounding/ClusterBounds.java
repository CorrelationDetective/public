package bounding;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import streaming.index.IndexInstruction;

@RequiredArgsConstructor
public class ClusterBounds {
    @NonNull @Getter @Setter public double LB;
    @NonNull @Getter @Setter public double UB;
    @NonNull @Getter @Setter public double maxLowerBoundSubset;

//    Cluster pairs that make up the LB, packaged as IndexInstructions to indicate whether to index the lb or ub extrema pair
    @NonNull @Getter @Setter public FastArrayList<IndexInstruction> lbIndexInstructions;

//    Cluster pairs that make up the UB, packaged as IndexInstructions to indicate whether to index the lb or ub extrema pair
    @NonNull @Getter @Setter public FastArrayList<IndexInstruction> ubIndexInstructions;
}