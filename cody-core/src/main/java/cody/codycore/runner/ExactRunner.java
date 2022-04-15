package cody.codycore.runner;

import cody.codycore.Configuration;
import cody.codycore.Preprocessor;
import cody.codycore.Validator;
import cody.codycore.candidate.CheckedColumnCombination;
import cody.codycore.candidate.ColumnCombination;
import cody.codycore.candidate.ColumnCombinationUtils;
import cody.codycore.pruning.ComponentPruner;
import cody.codycore.pruning.PrunerFactory;
import com.google.common.base.Stopwatch;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExactRunner extends BaseRunner {

    public ExactRunner(@NonNull Configuration configuration) {
        super(configuration);
    }

    /**
     * Run the approximate Cody algorithm with the set configuration
     * When finished, the results can be retrieved with getResultSet
     */
    @Override
    public void run() {
        Stopwatch completeWatch = Stopwatch.createStarted();
        log.trace("Start running exact Cody algorithm with configuration: {}", this.configuration);

        Stopwatch prepareWatch = Stopwatch.createStarted();
        Preprocessor preprocessor = new Preprocessor(this.configuration);
        preprocessor.run();
        log.trace("Preprocessing took: {} ms", prepareWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        Stopwatch validatorWatch = Stopwatch.createStarted();
        Validator validator = new Validator(this.configuration, preprocessor.getColumnPlis(),
                preprocessor.getNRows(), preprocessor.getRowCounts());
        log.trace("Unary candidate validation took: {} ms", validatorWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        Stopwatch prunerWatch = Stopwatch.createStarted();
        ComponentPruner pruner = PrunerFactory.create(this.configuration, validator.getGraphView());
        pruner.run();
        log.trace("Expanding unary to maximal Cody took: {} ms", prunerWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        Stopwatch postProcessingWatch = Stopwatch.createStarted();
        for (ColumnCombination c : pruner.getResultSet().values())
            this.resultSet.add(validator.checkColumnCombination(c));

        this.resultSet = this.resultSet.stream().map(c -> ColumnCombinationUtils.inflateDuplicateColumns(c,
                preprocessor.getColumnIndexToDuplicatesMapping())).toList();
        log.trace("Candidate post-processing took: {} ms", postProcessingWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        log.trace("Complete approximate Cody algorithm took: {} ms",
                completeWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        log.trace("ResultSet with {} Codys:", this.getResultSet().size());
        for (CheckedColumnCombination c : this.getResultSet())
            log.info("{}", c);

        System.out.println(
                configuration.getPath() + "," +
                (0.000001f * (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) + "," +
                configuration.getMinSupport() + "," +
                configuration.getNullValue() + "," +
                preprocessor.getNRows() + "," +
                preprocessor.nRowsDistinct + "," +
                preprocessor.columnPlisMutable.size() + "," +
                preprocessor.getColumnPlis().size() + "," +
                resultSet.size() + "," +
                0 + "," +
                prepareWatch.elapsed(TimeUnit.MILLISECONDS) + "," +
                validatorWatch.elapsed(TimeUnit.MILLISECONDS)  + "," +
                prunerWatch.elapsed(TimeUnit.MILLISECONDS) + "," +
                0 + "," +
                postProcessingWatch.elapsed(TimeUnit.MILLISECONDS) + "," +
                completeWatch.elapsed(TimeUnit.MILLISECONDS)
        );
    }

    public static void main(String[] args) {
        List<String> paths = List.of(
                "~/datasets/Building_Permits.csv",
                "~/datasets/ENTYTYSRCGEN.csv",
                "~/datasets/flight_1k.csv",
                "~/datasets/ncvoters.tsv",
                "~/datasets/NOHSS_Adult_Indicators.csv",
                "~/datasets/nyc_collision_factors_large.csv",
                "~/datasets/PDBX_DATABASE_STATUS.csv",
                "~/datasets/plista_1k.csv",
                "~/datasets/SG_BIOENTRY.csv",
                "~/datasets/U.S._Chronic_Disease_Indicators__CDI_.csv",
                "~/datasets/spk/kurzgutachten.csv",
                "~/datasets/spk/bodenwert.csv",
                "~/datasets/spk/sachwert.csv",
                "~/datasets/spk/nutzungen.csv"
        );

        List<Character> dels = List.of(
                ',',
                ';',
                ';',
                '\t',
                ',',
                ',',
                ';',
                ';',
                ';',
                ',',
                ',',
                ',',
                ',',
                ','
        );

        List<String> nulls = List.of(
                "",
                "?",
                "",
                "",
                "",
                "",
                "?",
                "",
                "",
                "",
                "0",
                "0",
                "0",
                "0"
        );

        int numRuns = 5;

        for (int p = 0; p < paths.size(); p++) {
            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(paths.get(p));
                config.setDelimiter(dels.get(p));
                config.setNullValue(nulls.get(p));
                config.setNoCliqueSearch(false);
                config.setMinSupport(1.0);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
            }
        }
    }
}
