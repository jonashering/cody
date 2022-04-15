package cody.codycore;

import cody.codycore.candidate.CheckedColumnCombination;
import cody.codycore.candidate.ColumnCombination;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Preprocessor {

    private final Configuration configuration;
    private final Object2IntLinkedOpenHashMap<IntList> rowDeduplicator;
    public List<MutableRoaringBitmap> columnPlisMutable;
    private String[] columnIndexToNameMapping;
    public int nRowsDistinct;
    public int numNulls = 0;

    /**
     * Contains a PLIs per column, a bit in PLI is set to true if cell for that row is null
     * Immutable representation of columnPlisMutable for performance reasons
     */
    @Getter private List<ImmutableRoaringBitmap> columnPlis;

    /**
     * Indicates which columns are duplicates
     * Index maps to a list of all duplicate indices in original dataset
     */
    @Getter private List<List<Integer>> columnIndexToDuplicatesMapping;


    /**
     * Indicates the total number of rows (also counting duplicates)
     */
    @Getter private int nRows;

    /**
     * Maps each row index to how often that row pattern has been found
     */
    @Getter private int[] rowCounts;

    public Preprocessor(@NonNull Configuration configuration) {
        this.configuration = configuration;
        this.rowDeduplicator = new Object2IntLinkedOpenHashMap<>();
        this.nRows = 0;
        this.nRowsDistinct = 0;
    }

    /**
     * Read the dataset, deduplicate rows and columns
     * When finished, results can be retrieved with respective getters
     */
    public void run() {
        CsvParser parser = this.initializeParser();

        // Initialize data structures
        String[] firstLine = parser.parseNext();
        List<MutableRoaringBitmap> dataset = new ArrayList<>(firstLine.length);
        for (int i = 0; i < firstLine.length; i++) {
            dataset.add(new MutableRoaringBitmap());
            this.columnPlisMutable = ImmutableList.copyOf(dataset);
        }

        // Start parsing the file
        if (this.configuration.isNoHeader()) {
            this.addRow(firstLine);
        } else {
            this.columnIndexToNameMapping = firstLine;
        }

        String[] nextRow;
        while ((nextRow = parser.parseNext()) != null) {
            this.addRow(nextRow);
            this.nRows++;
        }

        this.transformRows();
        this.transformColumns();
        log.trace("Deduplicated {} rows to {}, {} columns to {}", this.nRows, this.nRowsDistinct,
                this.columnPlisMutable.size(), this.columnPlis.size());
        //System.out.println("DS:" + numNulls / ((double) nRows * columnPlisMutable.size()) + "," + Arrays.toString
        // (columnPlisMutable.stream().mapToDouble(i -> i.getCardinality() / (double) nRows).toArray()));
    }

    private void addRow(String[] row) {
        IntList nullCols = new IntArrayList();
        for (int i = 0; i < row.length; i++)
            if (row[i].equals(this.configuration.getNullValue())){
                nullCols.add(i);
                numNulls++;
            }

        if (this.rowDeduplicator.containsKey(nullCols) && !configuration.noDedup) {
            this.rowDeduplicator.addTo(nullCols, 1);
        } else {
            for (int i : nullCols)
                this.columnPlisMutable.get(i).add(this.nRowsDistinct);
            this.nRowsDistinct++;
            if (configuration.noDedup)
                nullCols.add(this.nRowsDistinct + 100);
            this.rowDeduplicator.put(nullCols, 1);
        }
    }

    private CsvParser initializeParser() {
        File file = new File(this.configuration.getPath());
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (IOException e) {
            log.error("Fatal error reading {}. Now exiting.", this.configuration.getPath(), e);
            System.exit(1);
        }

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(this.configuration.getDelimiter());
        settings.getFormat().setQuote(this.configuration.getQuoteChar());
        settings.setNumberOfRowsToSkip(this.configuration.getSkipLines());
        settings.setNumberOfRecordsToRead(this.configuration.getRowLimit());
        settings.setNullValue(""); // empty cells will be converted to String "" instead of being null
        settings.setEmptyValue("");
        if (this.configuration.getColLimit() != -1)
            settings.selectIndexes(IntStream.range(0, this.configuration.getColLimit()).boxed().toArray(Integer[]::new));
        if (this.configuration.getColLimitRandom() != -1) {
            List<Integer> indices =
                    IntStream.range(0, this.configuration.getColLimitRandomMax()).boxed().collect(Collectors.toList());
            Collections.shuffle(indices);
            settings.selectIndexes(indices.stream().limit(configuration.getColLimitRandom()).toArray(Integer[]::new));
        }

        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(reader);

        return parser;
    }

    private void transformColumns() {
        ListMultimap<ImmutableRoaringBitmap, Integer> deduplicator =
                this.createDuplicateColumnMapping();

        this.columnPlis = ImmutableList.copyOf(deduplicator.keySet());

        if (configuration.noDedup) {
            List<ImmutableRoaringBitmap> newList = new ArrayList<>();
            for (ImmutableRoaringBitmap bm : deduplicator.keySet()) {
                for (int i : deduplicator.get(bm))
                    newList.add(bm);
            }
            this.columnPlis = ImmutableList.copyOf(newList);
        }

        // FIXME: figure out how to collect values as List of Lists
        List<List<Integer>> mapper = new ArrayList<>(this.columnPlis.size());
        for (ImmutableRoaringBitmap b : this.columnPlis)
            mapper.add(ImmutableList.copyOf(deduplicator.get(b)));
        this.columnIndexToDuplicatesMapping = ImmutableList.copyOf(mapper);

        if (log.isDebugEnabled()) {
            log.debug("Duplicate columns: {}", mapper);
            for (ImmutableRoaringBitmap b : this.columnPlis)
                log.debug("Column null values: {}/{} ", b.getCardinality(), this.nRowsDistinct);
        }
    }

    private ListMultimap<ImmutableRoaringBitmap, Integer> createDuplicateColumnMapping() {
        ListMultimap<ImmutableRoaringBitmap, Integer> deduplicator =
                MultimapBuilder.linkedHashKeys().arrayListValues().build();
        for (int i = 0; i < this.columnPlisMutable.size(); i++)
            deduplicator.put(this.columnPlisMutable.get(i).toImmutableRoaringBitmap(), i);

        return deduplicator;
    }

    private ListMultimap<ImmutableRoaringBitmap, Integer> createDuplicateColumnMapping(double maxError) {
        ListMultimap<ImmutableRoaringBitmap, Integer> deduplicator =
                MultimapBuilder.linkedHashKeys().arrayListValues().build();
        for (int i = 0; i < this.columnPlisMutable.size(); i++) {
            boolean alreadyHasRepresentative = false;
            ImmutableRoaringBitmap toCheck = this.columnPlisMutable.get(i).toImmutableRoaringBitmap();
            for (ImmutableRoaringBitmap representative : deduplicator.keySet()) {
                if (this.calculateSupport(ImmutableRoaringBitmap.xor(representative, toCheck)) <= maxError) {
                    deduplicator.put(representative, i);
                    alreadyHasRepresentative = true;
                    break;
                }
            }
            if (!alreadyHasRepresentative)
                deduplicator.put(toCheck, i);
        }

        return deduplicator;
    }

    private double calculateSupport(ImmutableRoaringBitmap pli) {
        double support = 0.0;
        if (pli.cardinalityExceeds(0)) {
            for (int index : pli)
                support += this.rowCounts[index];
        }

        return support / (double) this.nRows;
    }

    private void transformRows() {
        // Actual deduplication happens in addRow(), setting results properly here
        this.rowCounts = this.rowDeduplicator.values().toIntArray();
    }

    /**
     * Pretty print a candidate (i.e. map indices to column names and revert deduplication)
     *
     * @param c ColumnCombination to pretty print
     * @return String representation
     */
    public String prettyPrint(@NonNull ColumnCombination c) {
        // TODO
        return c.toString();
    }

    /**
     * Pretty print a candidate (i.e. map indices to column names and revert deduplication)
     *
     * @param c CheckedColumnCombination to pretty print
     * @return String representation
     */
    public String prettyPrint(@NonNull CheckedColumnCombination c) {
        // TODO
        return this.prettyPrint(c.getColumnCombination()) + ": Support = XX";
    }
}
