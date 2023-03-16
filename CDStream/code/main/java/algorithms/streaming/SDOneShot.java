package algorithms.streaming;

import _aux.StatBag;
import algorithms.AlgorithmEnum;
import algorithms.statics.SimilarityDetective;
import core.Parameters;
import queries.ResultSet;
import similarities.functions.EuclideanSimilarity;
import similarities.functions.PearsonCorrelation;
import streaming.LinearRegressor;

import java.util.logging.Level;

public class SDOneShot extends StreamingAlgorithm {
    private SimilarityDetective sd;
    public SDOneShot(Parameters par) {
        super(par);
        this.runtimePredictor = new LinearRegressor(false);
        par.setActiveAlgorithm(this);

        sd = new SimilarityDetective(par);
    }

    @Override
    public ResultSet run() {
//        Turn off logger for SD
        Level oldLevel = par.LOGGER.getLevel();
        par.LOGGER.setLevel(Level.WARNING);

        ResultSet resultSet = sd.run();

        par.LOGGER.setLevel(oldLevel);

        return resultSet;
    }
}
