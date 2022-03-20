package cody.codycore.runner;

import cody.codycore.Configuration;
import cody.codycore.candidate.CheckedColumnCombination;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseRunner {

    protected final Configuration configuration;

    /**
     * Contains all maximal valid ColumnCombinations
     */
    @Getter protected List<CheckedColumnCombination> resultSet;

    public BaseRunner(@NonNull Configuration configuration) {
        this.configuration = configuration;
        this.resultSet = new ArrayList<>();
    }

    public abstract void run();
}
