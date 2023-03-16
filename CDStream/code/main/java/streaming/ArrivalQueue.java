package streaming;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.PriorityQueue;

public class ArrivalQueue {
    private final PriorityQueue<Arrival> queue = new PriorityQueue<>(Arrival::compareTo);
    @Getter public double maxT = 0;

    public void add(Arrival arrival) {
        queue.add(arrival);
        maxT = Math.max(maxT, arrival.getT());
    }

    public void addAll(Collection<Arrival> arrivals) {
        queue.addAll(arrivals);
    }

    public Arrival poll() {
        return queue.poll();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public Arrival peek() {
        return queue.peek();
    }

    public void clear() {
        queue.clear();
    }

    public void remove(Arrival arrival) {
        queue.remove(arrival);
    }

    public void removeIf(Arrival arrival) {
        queue.removeIf(arrival::equals);
    }

    public boolean contains(Arrival arrival) {
        return queue.contains(arrival);
    }

//    -------------------------------------------------------------------------
}
