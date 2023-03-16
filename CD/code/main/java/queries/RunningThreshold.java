package queries;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.logging.Logger;

@RequiredArgsConstructor
public class RunningThreshold {
    @NonNull
    public double threshold;
    private boolean locked = false;

    @NonNull private Logger LOGGER;

    public String toString(){return String.format("%.6f", threshold);}

    public double get(){return threshold;}

    synchronized public void setThreshold(double newThreshold) {
        if (newThreshold > threshold) {
            threshold = newThreshold;
//            LOGGER.fine("New correlation threshold: " + this.threshold);
        }
    }

}
