package cody.codycore.candidate;

import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class CheckedColumnCombination {

    /**
     * Holds the respective ColumnCombination object
     */
    @Delegate(types = ColumnCombination.class)
    ColumnCombination columnCombination;

    /**
     * Indicates the ColumnCombinations support as rows where Cody is valid / total rows
     */
    double support;
}
