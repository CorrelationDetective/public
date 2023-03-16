package streaming;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Arrival {
    @NonNull @Getter public final double t;
    @NonNull @Getter public final double val;
    @NonNull @Getter public final int key;

    public String toString(){return String.format("Arrival: t=%d, val=%.3f, key=%d", (int) t, val, key);}

    public int compareTo(Arrival other) {
        return Double.compare(t, other.t);
    }

    public boolean equals(Arrival other) {
        return t == other.t && val == other.val && key == other.key;
    }
}
