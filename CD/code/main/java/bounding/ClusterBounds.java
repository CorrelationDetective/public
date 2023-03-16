package bounding;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class ClusterBounds {
    @NonNull @Getter @Setter public double LB;
    @NonNull @Getter @Setter public double UB;
    @NonNull @Getter @Setter public double maxLowerBoundSubset;
}