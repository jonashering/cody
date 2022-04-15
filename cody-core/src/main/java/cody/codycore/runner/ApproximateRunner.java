package cody.codycore.runner;

import ch.javasoft.bitset.search.TreeSearch;
import cody.codycore.Configuration;
import cody.codycore.Preprocessor;
import cody.codycore.Validator;
import cody.codycore.candidate.CheckedColumnCombination;
import cody.codycore.candidate.ColumnCombination;
import cody.codycore.candidate.ColumnCombinationUtils;
import cody.codycore.pruning.ComponentPruner;
import cody.codycore.pruning.PrunerFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ApproximateRunner extends BaseRunner {

    public ApproximateRunner(@NonNull Configuration configuration) {
        super(configuration);
    }

    /**
     * Run the approximate Cody algorithm with the set configuration
     * When finished, the results can be retrieved with getResultSet
     */
    @Override
    public void run() {
        Stopwatch completeWatch = Stopwatch.createStarted();
        log.trace("Start running approximate Cody algorithm with configuration: {}", this.configuration);

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

        Multimap<Integer, ColumnCombination> optimisticCandidates = pruner.getResultSet();
        int maxCardinality = optimisticCandidates.isEmpty() ? 0 : Collections.max(optimisticCandidates.keySet());
        log.trace("Estimating upper bounds took: {} ms", prunerWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        Stopwatch latticeTraversal = Stopwatch.createStarted();
        List<ColumnCombination> currentLevelCandidates = new ArrayList<>();
        TreeSearch maximalValidColumnCombinations = new TreeSearch();
        int totalCandidates = 0;
        for (int level = maxCardinality; level >= 2; level--) {
            currentLevelCandidates.addAll(optimisticCandidates.get(level));
            totalCandidates += currentLevelCandidates.size();

            log.trace("At level: {} with: {} candidates", level, currentLevelCandidates.size());
            List<CheckedColumnCombination> checkedCurrentLevelCandidates = currentLevelCandidates
                    .parallelStream()
                    .map(validator::checkColumnCombination)
                    .toList();

            checkedCurrentLevelCandidates
                    .stream()
                    .filter(c -> c.getSupport() >= this.configuration.getMinSupport())
                    .forEach(c -> {
                        maximalValidColumnCombinations.add(c.getColumns());
                        this.resultSet.add(c);
                        log.debug("Found valid candidate: {}", c);
                    });

            currentLevelCandidates = checkedCurrentLevelCandidates
                    .parallelStream()
                    .filter(c -> c.getSupport() < this.configuration.getMinSupport())
                    .flatMap(c -> ColumnCombinationUtils.getImmediateSubsets(c).stream())
                    .distinct()
                    .filter(c -> maximalValidColumnCombinations.findSuperSet(c.getColumns()) == null)
                    .collect(Collectors.toList());
        }
        log.trace("Candidate validation took: {} ms", latticeTraversal.stop().elapsed(TimeUnit.MILLISECONDS));

        Stopwatch postProcessingWatch = Stopwatch.createStarted();
        this.resultSet = this.resultSet.stream().map(c -> ColumnCombinationUtils.inflateDuplicateColumns(c,
                preprocessor.getColumnIndexToDuplicatesMapping())).toList();
        log.trace("Candidate post-processing took: {} ms", postProcessingWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        log.trace("Complete approximate Cody algorithm took: {} ms",
                completeWatch.stop().elapsed(TimeUnit.MILLISECONDS));

        log.trace("ResultSet with {} Codys:", this.resultSet.size());
        for (CheckedColumnCombination c : this.resultSet)
            log.trace("{}", c);

        System.out.println(configuration.getPath() + "," +
                (0.000001f * (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) + "," +
                           configuration.getMinSupport() + "," +
                           configuration.getNullValue() + "," +
                           preprocessor.getNRows() + "," +
                           preprocessor.nRowsDistinct + "," +
                           preprocessor.columnPlisMutable.size() + "," +
                           preprocessor.getColumnPlis().size() + "," +
                           resultSet.size() + "," +
                           totalCandidates + "," +
                           prepareWatch.elapsed(TimeUnit.MILLISECONDS) + "," +
                           validatorWatch.elapsed(TimeUnit.MILLISECONDS)  + "," +
                           prunerWatch.elapsed(TimeUnit.MILLISECONDS) + "," +
                           latticeTraversal.elapsed(TimeUnit.MILLISECONDS) + "," +
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
        List<Double> minSupps = List.of(.9, .95, .99);

        for (int p = 0; p < paths.size(); p++) {
            for (double supp : minSupps) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(paths.get(p));
                    config.setDelimiter(dels.get(p));
                    config.setNullValue(nulls.get(p));
                    config.setNoCliqueSearch(false);
                    config.setMinSupport(supp);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                }
            }
        }
    }
}
