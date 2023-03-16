package _aux;

import _aux.Stage;
import _aux.Tuple3;
import _aux.lib;
import _aux.lists.FastLinkedList;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;

import java.util.function.Supplier;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class StageRunner {
    private final Logger LOGGER;

//    <Name, Duration, ExpectedDuration>
    public final FastLinkedList<Stage> stageDurations = new FastLinkedList<>();
    public double stageDurationSum = 0;

    public <T> T run(String name, Supplier<T> stage, StopWatch stopWatch) {
        LOGGER.fine(String.format("----------- %d. %s --------------",stageDurations.size(), name));

        try {
            return stage.get();
        } finally {
            stopWatch.split();
            double splitTime = lib.nanoToSec(stopWatch.getSplitNanoTime());
            double splitDuration = splitTime - stageDurationSum;
            stageDurations.add(new Stage(name, splitDuration));
            stageDurationSum += splitDuration;
        }

    }

    public void run(String name, Runnable stage, StopWatch stopWatch) {
        LOGGER.fine(String.format("----------- %d. %s --------------",stageDurations.size(), name));

        long start = System.currentTimeMillis();
        try {
            stage.run();
        } finally {
            stopWatch.split();
            double splitTime = lib.nanoToSec(stopWatch.getSplitNanoTime());
            double splitDuration = stageDurations.isEmpty() ? splitTime : splitTime - stageDurations.getLast().getDuration();
            stageDurations.add(new Stage(name, splitDuration));
        }
    }
}
