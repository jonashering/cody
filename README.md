# cody

Cody is a data profiling algorithm to discover Complementation Dependencies, a pattern of missing values, in datasets.

### Installation
```bash
# Use your preferred package manager to install a JDK and Maven
# We recommend using Java 17 or later, however, Cody works with at least Java 8:
$ java -version

# Then build a fatjar
$ mvn clean package
```

### Run as CLI App
```bash
# Run the algorithm
$ java -jar ./cody-core/target/cody-core-1.2-SNAPSHOT.jar --path ../some_dataset.csv --supp 0.99
> [INFO] Running approximat Cody algorithm ...
> ... # Results will be logged at the end

# For large datasets, you may need to increase heap size (VisualVM is a good helper here)
$ java -Xmx4G -jar ./cody-core/...

# See all parameters with --help flag
$ java -jar ./cody-core/... --help
> Usage: <main class> [options]
>  Options:
>    --del, -d
>      Delimiter used in the dataset
>      Default: ,
>    --help, -h
>      Show this help page
>    --no-cliques
>      Disable clique search for approximate Cody discovery
>      Default: false
>    --no-header
>      First line already contains data, column names are indices
>      Default: false
>    --null
>      Custom null value in dataset
>      Default: <empty string>
>  * --path, -p
>      Relative Path to the CSV file containing the dataset
>    --quote
>      Custom quote char in dataset
>      Default: \"
>    --skip
>      Number of lines to skip when reading the dataset
>      Default: 0
>    --supp, -s
>      Minimum support, set to 1.0 for exact Cody search
>      Default: 1.0
```

### Run with the Metanome Tool
[Metanome](https://github.com/HPI-Information-Systems/Metanome) is a unified tool to run all kinds of data profiling algorithms developed at Hasso-Plattner-Institute. Follow the installation instructions on the Metanome page or use the pre-packaged release from the [HPI website](https://hpi.de/naumann/projects/data-profiling-and-analytics/metanome-data-profiling.html).

When you ran `mvn clean package` during the installation, you also built a fatjar of the Cody-Tool supporting the Metanome interface. You find it at `./cody-metanome/target/cody-metanome-1.2-SNAPSHOT.jar` or similar.

To install it in your Metanome distribution, follow the instructions on their Github page.

*Note:* Some configuration parameters may only be available through the CLI app.
