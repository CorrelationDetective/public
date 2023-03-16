package algorithms.streaming;

import _aux.StatBag;
import algorithms.statics.baselines.SmartBaseline;
import core.Parameters;
import queries.ResultSet;

import java.util.logging.Level;

public class BaselineStream extends StreamingAlgorithm {
    private SmartBaseline baseline;
    public BaselineStream(Parameters par) {
        super(par);
        baseline = new SmartBaseline(par);
    }

    @Override
    public ResultSet run() {
//        Turn off logger for SD
        Level oldLevel = par.LOGGER.getLevel();
        par.LOGGER.setLevel(Level.WARNING);

        ResultSet resultSet = baseline.run();

        par.LOGGER.setLevel(oldLevel);
        return resultSet;
    }
}
