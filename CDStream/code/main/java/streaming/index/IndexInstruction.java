package streaming.index;

import bounding.ClusterPair;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

// Combination of a clusterPair and a boolean indicating to index based on the lb or ub dist extrema pair
@RequiredArgsConstructor
public class IndexInstruction {
    @NonNull public ClusterPair clusterPair;
    @NonNull public boolean indexOnUb;
}
