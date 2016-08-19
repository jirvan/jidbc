package com.jirvan.jidbc.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jirvan.io.OutputWriter;
import com.jirvan.lang.NotFoundRuntimeException;
import com.jirvan.util.Jdbc;
import com.jirvan.util.Strings;
import com.jirvan.util.Utl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class ForeignKeys {

    public static final String USAGE = "\nUsage:\n\n   java -jar <jar file> <dbConnectString> <rootTable> <fkeysToIgnore>\n" +
                                       "       where dbConnectString is of the form \"postgresql:<user>/<password>@<server>[:port]/<database>\"\n" +
                                       "                                         or \"oracle:<user>/<password>@<server>/<service>\"\n" +
                                       "                                         or \"sqlserver:<user>/<password>@<server>[\\instance][:port]/<database>\"\n" +
                                       "                                         or \"sqlite:<database file>" +
                                       "       and fkeysToIgnore is a comma seperated list of foreign keys to ignore";

    /**
     * This main doesn't really belong here, but is useful for testing
     **/
    public static void main(String[] args) {
        if (args.length == 2) {
//            ForeignKeys.printDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1]);
//            ForeignKeys.printDeleteSqlForDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1]);
            ForeignKeys.printDeleteJidbcCodeForDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1]);
//            System.out.printf("\n\n\n");
//            ForeignKeys.printDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1]);
        } else if (args.length == 3) {
            ForeignKeys.printDeleteJidbcCodeForDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1], args[2].split(","));
            System.out.printf("\n\n\n");
            ForeignKeys.printDependencyTreeRootedAt(new OutputWriter(System.out), Jdbc.getDataSource(args[0]), args[1]);
        } else {
            System.err.println(USAGE);
        }
    }

    public static Table getDependencyTreeRootedAt(DataSource dataSource, String rootTable) {
        return new ForeignKeys(dataSource).extractDependencyTree(rootTable, true).treeRoot;
    }

    public static void printDependencyTreeRootedAt(OutputWriter output, DataSource dataSource, String rootTable) {
        printDependencyTreeNode(output, new ForeignKeys(dataSource).extractDependencyTree(rootTable, true).treeRoot);
    }

    public static void printDeleteSqlForDependencyTreeRootedAt(OutputWriter output, DataSource dataSource, String rootTable) {
        printDeleteSqlForDependencyTreeRootedAt(output, new ForeignKeys(dataSource), rootTable);
    }

    public static void printDeleteJidbcCodeForDependencyTreeRootedAt(OutputWriter output, DataSource dataSource, String rootTable) {
        printDeleteJidbcCodeForDependencyTreeRootedAt(output, new ForeignKeys(dataSource), rootTable);
        output.printf("\n\n/* Dependency tree\n\n");
        ForeignKeys.printDependencyTreeRootedAt(output, dataSource, rootTable);
        output.printf("\n*/\n");
    }

    public static void printDeleteJidbcCodeForDependencyTreeRootedAt(OutputWriter output, DataSource dataSource, String rootTable, String[] fkeysToIgnore) {
        printDeleteJidbcCodeForDependencyTreeRootedAt(output, new ForeignKeys(dataSource, fkeysToIgnore), rootTable, fkeysToIgnore);
        output.printf("\n\n/* Dependency tree\n\n");
        ForeignKeys.printDependencyTreeRootedAt(output, dataSource, rootTable);
        output.printf("\n*/\n");
    }

    public static abstract class Table {

        @JsonIgnore public TableDetails fullDetails;

        public Table(TableDetails fullDetails) {
            this.fullDetails = fullDetails;
        }

        public String getDisplayName() {
            return fullDetails.getDisplayName();
        }

    }

    public static class TablePrimaryReference extends Table {
        public List<Table> dependentTables = new ArrayList<>();

        public TablePrimaryReference(TableDetails fullDetails) {
            super(fullDetails);
        }
    }

    public static class TableSecondaryReference extends Table {
        public TableSecondaryReference(TableDetails fullDetails) {
            super(fullDetails);
        }
    }

    public static class TableCirularReference extends Table {
        public TableCirularReference(TableDetails fullDetails) {
            super(fullDetails);
        }
    }

    //======================== Everything below here is private ========================//


    private DataSource dataSource;
    private List<TableDetails> allTables;
    private String tableTableType;

    private ForeignKeys(DataSource dataSource, String... fkeysToIgnore) {
        this.dataSource = dataSource;
        validateTableTableType();
        allTables = getAllTables();
        for (TableDetails table : this.allTables) {
            addForeignKeyReferencesOtherThanFkeysToIgore(table, fkeysToIgnore);
        }
    }

    private static void printDeleteSqlForDependencyTreeRootedAt(OutputWriter output, ForeignKeys foreignKeys, String rootTable) {


        TablePrimaryReference remainingTree = foreignKeys.extractDependencyTree(rootTable, false).treeRoot;
        List<TableDetails> tables = null;

        int pass = 1;
        while (tables == null || tables.size() > 0) {

            // Take and sort tables from tree that have no dependencies (any more)
            tables = foreignKeys.takeNodesWithNoDependenciesFromTree(remainingTree);
            Collections.sort(tables, new Comparator<TableDetails>() {
                public int compare(TableDetails o1, TableDetails o2) {
                    return o1.tableName.compareTo(o2.tableName);
                }
            });

            // Output the delete lines for this tables
            if (tables.size() > 0) {
                output.printf("\n-- " + formatOrdinal(pass++) + " pass\n");
                for (TableDetails table : tables) {
                    output.printf("delete from %s where zzz = zzzzz;\n", table.getDisplayName());
                }
            }

        }

        if (remainingTree.dependentTables.size() > 0) {

            output.printf("\n***** Circular references *****\n" +
                          "The following tables could not be deleted from due to circular references\n\n");
            printDependencyTreeNode(output, remainingTree);

        } else {
            output.printf("\n-- Final pass\n");
            output.printf("delete from %s where zzz = zzzzz;\n", remainingTree.getDisplayName());

        }

    }

    private static void printDeleteJidbcCodeForDependencyTreeRootedAt(OutputWriter output, ForeignKeys foreignKeys, String rootTable, String... fkeysToIgnore) {


        ExtractionResult extractionResult = foreignKeys.extractDependencyTree(rootTable, false);
        TablePrimaryReference remainingTree = extractionResult.treeRoot;
        List<TableDetails> tables = null;

        int pass = 1;
        while (tables == null || tables.size() > 0) {

            // Take and sort tables from tree that have no dependencies (any more)
            tables = foreignKeys.takeNodesWithNoDependenciesFromTree(remainingTree);
            Collections.sort(tables, new Comparator<TableDetails>() {
                public int compare(TableDetails o1, TableDetails o2) {
                    return o1.tableName.compareTo(o2.tableName);
                }
            });

            // Output the delete lines for this tables
            if (tables.size() > 0) {
                output.printf("\n// " + formatOrdinal(pass++) + " pass\n");
                for (TableDetails table : tables) {
                    output.printf("jidbc.executeUpdate(\"delete from %s where zzz = ?\", zzzzz);\n", table.getDisplayName());
                }
            }

        }

        if (fkeysToIgnore.length > 0) {

            output.printf("\n/* Ignored foreign keys\n" +
                          "   (handling of these will need to be coded manually, e.g. by setting\n" +
                          "    the appropriate fkey columns to null before the deletes are run).\n\n");
            for (String fkeyToIgnore : fkeysToIgnore) {
                output.printf("      %s\n", fkeyToIgnore);
            }
            output.printf("*/\n\n");
        }

        if (remainingTree.dependentTables.size() > 0) {

            output.printf("\n/* Circular references\n" +
                          "   The following tables could not be deleted from due to circular references\n\n");
            printDependencyTreeNode(output, remainingTree);
            output.printf("*/\n\n");

        } else {
            output.printf("\n// Final pass\n");
            output.printf("jidbc.executeUpdate(\"delete from %s where zzz = ?\", zzzzz);\n", remainingTree.getDisplayName());

        }

        if (extractionResult.leftoverTables.size() > 0) {

            output.printf("\n/* Left over tables\n" +
                          "   The following database tables were not in the dependency tree\n\n");
            for (String leftoverTable : extractionResult.leftoverTables) {
                output.printf("   %s\n", leftoverTable);
            }
            output.printf("*/\n\n");

        } else {
            output.printf("\n\n// There were not tables in the database that where not part of the dependency tree\n");

        }

    }

    public static String formatOrdinal(Integer integer) {
        return integer == null ? null : formatOrdinal(integer.longValue());
    }

    public static String formatOrdinal(Long integer) {
        if (integer == null) {
            return null;
        } else if (integer.toString().endsWith("0")) {
            return integer + "th";
        } else if (integer.toString().endsWith("1")) {
            return integer + "st";
        } else if (integer.toString().endsWith("2")) {
            return integer + "nd";
        } else if (integer.toString().endsWith("3")) {
            return integer + "rd";
        } else {
            return integer + "th";
        }
    }

    private static void printDependencyTreeNode(OutputWriter output, Table tableNode) {
        if (tableNode instanceof TableSecondaryReference) {
            output.printf("%s (duplicate)\n", tableNode.getDisplayName());
        } else if (tableNode instanceof TableCirularReference) {
            output.printf("%s (********** CIRCULAR reference **********)\n", tableNode.getDisplayName());
        } else {
            output.printf("%s\n", tableNode.getDisplayName());
            for (Table childNode : ((TablePrimaryReference) tableNode).dependentTables) {
                output.pushLinePrefix("    ");
                printDependencyTreeNode(output, childNode);
                output.popLinePrefix();
            }
        }
    }

    private List<TableDetails> takeNodesWithNoDependenciesFromTree(TablePrimaryReference tableNode) {
        List<TableDetails> takenTables = new ArrayList<>();
        List<Table> newDependentTables = new ArrayList<>();
        for (Table childNode : tableNode.dependentTables) {
            if (childNode instanceof TableCirularReference) {
                newDependentTables.add(childNode);
            } else if (childNode instanceof TablePrimaryReference) {
                if (isALeafNode(childNode)) {
                    takenTables.add(childNode.fullDetails);
                } else {
                    newDependentTables.add(childNode);
                    takenTables.addAll(takeNodesWithNoDependenciesFromTree((TablePrimaryReference) childNode));
                }
            }
        }
        tableNode.dependentTables = newDependentTables;
        return takenTables;
    }

    private boolean isALeafNode(Table childNode) {
        return ((TablePrimaryReference) childNode).dependentTables.size() == 0;
    }

    private static class ExtractionResult {
        public TablePrimaryReference treeRoot;
        public Set<String> leftoverTables = new TreeSet<>();
    }

    private ExtractionResult extractDependencyTree(String rootTable, boolean includeSecondaryReferences) {
        return extractDependencyTree(getTableFromList(allTables, rootTable), includeSecondaryReferences);
    }

    private ExtractionResult extractDependencyTree(TableDetails rootTable, boolean includeSecondaryReferences) {
        Set<String> placedTables = new TreeSet<>();
        placedTables.add(rootTable.getDisplayName());
        Set<String> parentTables = new TreeSet<>();
        parentTables.add(rootTable.getDisplayName());
        ExtractionResult extractionResult = new ExtractionResult();
        extractionResult.treeRoot = extractDependencyTree(parentTables, placedTables, rootTable, includeSecondaryReferences);
        for (TableDetails table : allTables) {
            if (!placedTables.contains(table.getDisplayName())) {
                extractionResult.leftoverTables.add(table.getDisplayName());
            }
        }
        return extractionResult;
    }

    private TablePrimaryReference extractDependencyTree(Set<String> parentTables, Set<String> placedTables, TableDetails table, boolean includeSecondaryReferences) {
        TablePrimaryReference tableNode = new TablePrimaryReference(table);
        for (TableDetails referencingTable : table.referencingTables) {
            if (parentTables.contains(referencingTable.getDisplayName())) {
                tableNode.dependentTables.add(new TableCirularReference(referencingTable));
            } else if (placedTables.contains(referencingTable.getDisplayName())) {
                if (includeSecondaryReferences) {
                    tableNode.dependentTables.add(new TableSecondaryReference(referencingTable));
                }
            } else {
                placedTables.add(referencingTable.getDisplayName());
                parentTables.add(referencingTable.getDisplayName());
                tableNode.dependentTables.add(extractDependencyTree(parentTables, placedTables, referencingTable, includeSecondaryReferences));
                parentTables.remove(referencingTable.getDisplayName());
            }
        }
        Collections.sort(tableNode.dependentTables, new Comparator<Table>() {
            public int compare(Table o1, Table o2) {
                return o1.getDisplayName().compareTo(o2.getDisplayName());
            }
        });

        return tableNode;
    }

    private void validateTableTableType() {
        List<String> supportedTableTypes = getSupportedTableTypes();
        if (Strings.isIn("TABLE", supportedTableTypes)) {
            tableTableType = "TABLE";
        } else if (Strings.isIn("table", supportedTableTypes)) {
            tableTableType = "table";
        } else {
            throw new RuntimeException("This database does not support tables of type \"TABLE\" or \"table\"");
        }
    }

    private List<TableDetails> getAllTables() {
        try {
            try (Connection conn = dataSource.getConnection()) {

                try (ResultSet resultSet = conn.getMetaData().getTables(null, "%", "%", new String[]{tableTableType})) {
                    List<TableDetails> tables = new ArrayList<>();
                    while (resultSet.next()) {
                        tables.add(new TableDetails(resultSet));
                    }
                    return tables;
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getSupportedTableTypes() {
        try {
            try (Connection conn = dataSource.getConnection()) {

                try (ResultSet resultSet = conn.getMetaData().getTableTypes()) {
                    List<String> tableTypes = new ArrayList<>();
                    while (resultSet.next()) {
                        tableTypes.add(resultSet.getString(1));
                    }
                    return tableTypes;
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addForeignKeyReferencesOtherThanFkeysToIgore(TableDetails table, String... fkeysToIgnore) {
        try {
            try (Connection conn = dataSource.getConnection()) {

                try (ResultSet resultSet = conn.getMetaData().getExportedKeys(table.catalogName, table.schemaName, table.tableName)) {
                    while (resultSet.next()) {
                        if (!Strings.isIn(resultSet.getString("FK_NAME"), fkeysToIgnore)) {
                            TableDetails fkTable = getTableFromList(this.allTables,
                                                                    resultSet.getString("FKTABLE_CAT"),
                                                                    resultSet.getString("FKTABLE_SCHEM"),
                                                                    resultSet.getString("FKTABLE_NAME"));
                            if (!listContainsTable(table.referencingTables, fkTable)) {
                                table.referencingTables.add(fkTable);
                            }
                            if (!listContainsTable(fkTable.referencedTables, table)) {
                                fkTable.referencedTables.add(table);
                            }
                        }
                    }
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private TableDetails getTableFromList(List<TableDetails> tableList, String tableName) {
        return getTableFromList(tableList, null, null, tableName);
    }

    private TableDetails getTableFromList(List<TableDetails> tableList, String schemaName, String tableName) {
        return getTableFromList(tableList, null, schemaName, tableName);
    }

    private TableDetails getTableFromList(List<TableDetails> tableList, String catalogName, String schemaName, String tableName) {
        TableDetails foundTable = null;
        for (TableDetails table : tableList) {
            if ((catalogName == null || Utl.areEqual(table.catalogName, catalogName))
                && (schemaName == null || Utl.areEqual(table.schemaName, schemaName))
                && Utl.areEqual(table.tableName, tableName)) {
                if (foundTable == null) {
                    foundTable = table;
                } else {
                    throw new RuntimeException(String.format("Table \"%s\" found in more than one schema (%s and %s)", table.tableName, foundTable.schemaName, table.schemaName));
                }
            }
        }
        if (foundTable != null) {
            return foundTable;
        } else {
            throw new NotFoundRuntimeException(String.format("Table \"%s%s.%s\" not found", catalogName == null ? "" : catalogName + ".", schemaName, tableName));
        }
    }

    private boolean listContainsTable(List<TableDetails> tableList, TableDetails table) {
        for (TableDetails listTable : tableList) {
            if (Utl.areEqual(listTable.catalogName, table.catalogName) && Utl.areEqual(listTable.schemaName, table.schemaName) && Utl.areEqual(listTable.tableName, table.tableName)) {
                return true;
            }
        }
        return false;
    }

    private static class TableDetails {

        public String catalogName;
        public String schemaName;
        public String tableName;
        public List<TableDetails> referencingTables = new ArrayList<>();
        public List<TableDetails> referencedTables = new ArrayList<>();

        public TableDetails() {}

        public TableDetails(ResultSet resultSet) throws SQLException {
            this.catalogName = resultSet.getString("TABLE_CAT");
            this.schemaName = resultSet.getString("TABLE_SCHEM");
            this.tableName = resultSet.getString("TABLE_NAME");
        }

        public String getDisplayName() {
            return (catalogName == null ? "" : catalogName + ".")
                   + (schemaName == null || "public".equals(schemaName) ? "" : schemaName + ".")
                   + tableName;
        }

    }

}
