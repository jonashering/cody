package cody.codycore;

import cody.codycore.runner.ApproximateRunner;
import cody.codycore.runner.ExactRunner;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.SimpleTimeLimiter;

import java.util.List;

public class EvaluationRunner {

    public record DatasetConfig(String path, Character del, String nullVal, int limit) {}

    public static List<DatasetConfig> datasets = List.of(
            new DatasetConfig("/home/jonas.hering/datasets/Building_Permits.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/ENTYTYSRCGEN.csv", ';', "?", -1),
            new DatasetConfig("/home/jonas.hering/datasets/flight_1k.csv", ';', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/ncvoters_large.tsv", '\t', "", 8000001),
            new DatasetConfig("/home/jonas.hering/datasets/ncvoters_large.tsv", '\t', "", 100001),
            new DatasetConfig("/home/jonas.hering/datasets/NOHSS_Adult_Indicators.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/nyc_collision_factors_large.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/nyc_collision_factors.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/PDBX_DATABASE_STATUS.csv", ';', "?", -1),
            new DatasetConfig("/home/jonas.hering/datasets/U.S._Chronic_Disease_Indicators__CDI_.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/datasets/SG_BIOENTRY.csv", ',', "", -1),
            new DatasetConfig("/home/jonas.hering/datasets/kurzgutachten.csv", ',', "0", 100001),
            new DatasetConfig("/home/jonas.hering/datasets/bodenwert.csv", ',', "0", -1),
            new DatasetConfig("/home/jonas.hering/datasets/sachwert.csv", ',', "0", -1),
            new DatasetConfig("/home/jonas.hering/datasets/nutzungen.csv", ',', "0", -1)
        );

    public static void runAllDatasets() {
        int numRuns = 3;
        List<Double> minSupps = List.of(.95, .99);

        for (DatasetConfig c : datasets) {
            for (double supp : minSupps) {
                if (c.path().equals("/home/jonas.hering/datasets/kurzgutachten.csv") && supp <= 0.95) continue;
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }
        }

        for (DatasetConfig c : datasets) {
            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(c.path());
                config.setDelimiter(c.del());
                config.setNullValue(c.nullVal());
                config.setRowLimit(c.limit());
                config.setMinSupport(1.0);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
                Runtime.getRuntime().gc();
            }
        }
    }

    public static void runRows() {
        int numRuns = 3;
        List<Double> minSupps = List.of(.95, .99);

        DatasetConfig c = datasets.get(3); // ncvoters_large_8m
        for (int rows = 1000001; rows < 8000001; rows += 1000000) {
            for (double supp : minSupps) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(rows);
                    config.setMinSupport(supp);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }

            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(c.path());
                config.setDelimiter(c.del());
                config.setNullValue(c.nullVal());
                config.setRowLimit(rows);
                config.setMinSupport(1.0);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
                Runtime.getRuntime().gc();
            }
        }

        c = datasets.get(6); // nyc_collisions
        for (int rows = 200001; rows < 1800001; rows += 200000) {
            for (double supp : minSupps) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(rows);
                    config.setMinSupport(supp);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }

            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(c.path());
                config.setDelimiter(c.del());
                config.setNullValue(c.nullVal());
                config.setRowLimit(rows);
                config.setMinSupport(1.0);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
                Runtime.getRuntime().gc();
            }
        }
    }

    public static void runCols() {
        int numRuns = 3;
        List<Double> minSupps = List.of(.95, .99);

        DatasetConfig c = datasets.get(4); // ncvoters_large_100k
        for (int cols = 10; cols < 91; cols += 10) {
            for (double supp : minSupps) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setColLimitRandom(cols);
                    config.setColLimitRandomMax(90);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }

            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(c.path());
                config.setDelimiter(c.del());
                config.setNullValue(c.nullVal());
                config.setRowLimit(c.limit());
                config.setMinSupport(1.0);
                config.setColLimitRandom(cols);
                config.setColLimitRandomMax(90);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
                Runtime.getRuntime().gc();
            }
        }

        c = datasets.get(11); // kurzgutachten
        for (int cols = 25; cols < 201; cols += 25) {
            for (double supp : minSupps) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setColLimitRandom(cols);
                    config.setColLimitRandomMax(90);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }

            for (int i = 0; i < numRuns; i++) {
                Configuration config = new Configuration();
                config.setPath(c.path());
                config.setDelimiter(c.del());
                config.setNullValue(c.nullVal());
                config.setRowLimit(c.limit());
                config.setMinSupport(1.0);
                config.setColLimitRandom(cols);
                config.setColLimitRandomMax(90);

                ExactRunner runner = new ExactRunner(config);
                runner.run();
                Runtime.getRuntime().gc();
            }
        }
    }

    public static void runSupport() {
        int numRuns = 3;
        List<DatasetConfig> selected = List.of(datasets.get(4), datasets.get(6));

        for (DatasetConfig c : selected) {
            for (double supp = 0.67; supp < 1.0; supp += 0.05) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();

                    if (supp == 0.67) supp = 0.65;
                }
            }
        }
    }

    public static void runCliqueVsComponent() {
        int numRuns = 3;
        List<DatasetConfig> selected = List.of(datasets.get(4), datasets.get(6));

        for (DatasetConfig c : selected) {
            for (double supp : List.of(0.9, 0.95, 0.99)) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setNoCliqueSearch(true);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }
        }

        System.out.println(",,,,,,,,,,,,,,,");

        for (DatasetConfig c : selected) {
            for (double supp : List.of(0.9, 0.95, 0.99)) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setNoCliqueSearch(false);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }
        }
    }

    public static void runDedup() {
        int numRuns = 3;
        List<DatasetConfig> selected = List.of(datasets.get(4), datasets.get(6));

        for (DatasetConfig c : selected) {
            for (double supp : List.of(0.9, 0.95, 0.99, 1.0)) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setNoDedup(true);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }
        }

        System.out.println(",,,,,,,,,,,,,,,");

        for (DatasetConfig c : selected) {
            for (double supp : List.of(0.9, 0.95, 0.99, 1.0)) {
                for (int i = 0; i < numRuns; i++) {
                    Configuration config = new Configuration();
                    config.setPath(c.path());
                    config.setDelimiter(c.del());
                    config.setNullValue(c.nullVal());
                    config.setRowLimit(c.limit());
                    config.setMinSupport(supp);
                    config.setNoDedup(false);

                    ApproximateRunner runner = new ApproximateRunner(config);
                    runner.run();
                    Runtime.getRuntime().gc();
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("path,memory,support,nullValue,rows,rowsD,cols,colsD,result,candidates,preprocessor," +
                "validator,pruner,traverser,postprocessor,total");
        if (args[0].equals("all")) {
            runAllDatasets();
        } else if (args[0].equals("rows")) {
            runRows();
        } else if (args[0].equals("cols")) {
            runCols();
        } else if (args[0].equals("dedup")) {
            runDedup();
        } else if (args[0].equals("clique")) {
            runCliqueVsComponent();
        } else if (args[0].equals("supp")) {
            runSupport();
        } else {
            System.out.println("UNKNOWN GOAL");
        }
    }
}
