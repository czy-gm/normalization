package normal;

public class Timer {

    private final long initTime;

    private long time;

    public Timer() {
        initTime = System.nanoTime();
        time = initTime;
    }

    public double getTimeAndReset() {
        long tmp = time;
        time = System.nanoTime();
        return (time - tmp) / 1000000.0;
    }

    public double getTime() {
        return (System.nanoTime() - time) / 1000000.0;
    }

    public void reset() {
        time = System.nanoTime();
    }

    public double totalTime() {
        return (System.nanoTime() - initTime) / 1000000.0;
    }

}
