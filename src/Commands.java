import static java.lang.System.out;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Commands {

    public static void parseUserCommand(String userString) {

        /*
         * commandTokens is an array of Strings that contains one token per array
         * element The first token can be used to determine the type of command The
         * other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userString.split(" ")));

        /*
         * This switch handles a very small list of hardcoded commands of known syntax.
         * You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0)) {
            case "show":
                if (commandTokens.get(1).equals("tables"))
                    parseUserCommand("select * from davisbase_tables");
                else if (commandTokens.get(1).equals("rowid")) {
                    DavisBaseBinaryFile.showRowId = true;
                    System.out.println("* Table Select will now include RowId.");
                } else
                    System.out.println("! I didn't understand the command: \"" + userString + "\"");
                break;
            case "select":
                parseString(userString);
                break;
            case "drop":
                dropTable(userString);
                break;
            case "create":
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userString);
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIdx(userString);
                break;
            case "update":
                parseUpdateTable(userString);
                break;
            case "insert":
                parseInsertTable(userString);
                break;
            case "delete":
                parseDeleteTable(userString);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                Settings.setExit(true);
                break;
            case "quit":
                Settings.setExit(true);
                break;
            case "test":
                test();
                break;
            default:
                System.out.println("! I didn't understand the command: \"" + userString + "\"");
                break;
        }
    }
    public static void parseString(String queryStr) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(queryStr.split(" ")));
        int i = 0;

        for (i = 1; i < queryTableTokens.size(); i++) {
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",")) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
        }

        MetaData tableMetaData = new MetaData(table_name);
        if(!tableMetaData.tableExists){
            System.out.println("! Table does not exist");
            return;
        }

        SpecialCondition condition = null;
        try {

            condition = getConditionFromQuery(tableMetaData, queryStr);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if (column_names.size() == 0) {
            column_names = tableMetaData.columnNames;
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(table_name), "r");
            DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);
            tableBinaryFile.selectRecords(tableMetaData, column_names, condition);
            tableFile.close();
        } catch (IOException exception) {
            System.out.println("! Error selecting columns from table");
        }

    }

    public static void parseCreateIdx(String createIdxStr) {
        ArrayList<String> createIndexTokens = new ArrayList<String>(Arrays.asList(createIdxStr.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !createIdxStr.contains("(")
                    || !createIdxStr.contains(")") && createIndexTokens.size() < 4) {
                System.out.println("! Syntax Error");
                return;
            }

            String tableName = createIdxStr
                    .substring(createIdxStr.indexOf("on") + 3, createIdxStr.indexOf("(")).trim();
            String columnName = createIdxStr
                    .substring(createIdxStr.indexOf("(") + 1, createIdxStr.indexOf(")")).trim();

            // check if the index already exists
            if (new File(TableUtils.getIndexFilePath(tableName, columnName)).exists()) {
                System.out.println("! Index already exists");
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(tableName), "rw");

            MetaData metaData = new MetaData(tableName);

            if (!metaData.tableExists) {
                System.out.println("! Invalid Table name");
                tableFile.close();
                return;
            }

            int columnOrdinal = metaData.columnNames.indexOf(columnName);

            if (columnOrdinal < 0) {
                System.out.println("! Invalid column name");
                tableFile.close();
                return;
            }


            // create index file
            RandomAccessFile indexFile = new RandomAccessFile(TableUtils.getIndexFilePath(tableName, columnName), "rw");
            Page.addNewPage(indexFile, PageType.LEAFINDEX, -1, -1);


            if (metaData.recordCount > 0) {
                BPlusOneTree bPlusOneTree = new BPlusOneTree(tableFile, metaData.rootPageNo, metaData.tableName);
                for (int pageNo : bPlusOneTree.getAllLeaves()) {
                    Page page = new Page(tableFile, pageNo);
                    BTree bTree = new BTree(indexFile);
                    for (TableRecord record : page.getPageRecords()) {
                        bTree.insertRow(record.getAttributes().get(columnOrdinal), record.rowId);
                    }
                }
            }

            System.out.println("* Index created on the column : " + columnName);
            indexFile.close();
            tableFile.close();

        } catch (IOException e) {

            System.out.println("! Error on creating Index");
            System.out.println(e);
        }

    }

    public static void parseUpdateTable(String updateString) {
        ArrayList<String> updateTokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));

        String table_name = updateTokens.get(1);
        List<String> columnsToUpdate = new ArrayList<>();
        List<String> valueToUpdate = new ArrayList<>();

        if (!updateTokens.get(2).equals("set") || !updateTokens.contains("=")) {
            System.out.println("! Syntax error");
            System.out.println(
                    "Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2; ");
            return;
        }

        String updateColInfoString = updateString.split("set")[1].split("where")[0];

        List<String> column_newValueSet = Arrays.asList(updateColInfoString.split(","));

        for (String item : column_newValueSet) {
            columnsToUpdate.add(item.split("=")[0].trim());
            valueToUpdate.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
        }

        MetaData metadata = new MetaData(table_name);

        if (!metadata.tableExists) {
            System.out.println("! Invalid Table name");
            return;
        }

        if (!metadata.columnExists(columnsToUpdate)) {
            System.out.println("! Invalid column name(s)");
            return;
        }

        SpecialCondition condition = null;
        try {

            condition = getConditionFromQuery(metadata, updateString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;

        }




        try {
            RandomAccessFile file = new RandomAccessFile(TableUtils.getTablePath(table_name), "rw");
            DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile(file);
            int noOfRecordsupdated = binaryFile.updateRecords(metadata, condition, columnsToUpdate, valueToUpdate);

            if(noOfRecordsupdated > 0)
            {
                List<Integer> allRowids = new ArrayList<>();
                for(ColumnInformation colInfo : metadata.columnNameAttrs)
                {
                    for(int i=0;i<columnsToUpdate.size();i++)
                        if(colInfo.columnName.equals(columnsToUpdate.get(i)) &&  colInfo.hasIndex)
                        {

                            // when there is no condition, All rows in the column gets updated the index value point to all rowids
                            if(condition == null)
                            {
                                //Delete the index file. TODO

                                if(allRowids.size() == 0)
                                {
                                    BPlusOneTree bPlusOneTree = new BPlusOneTree(file, metadata.rootPageNo, metadata.tableName);
                                    for (int pageNo : bPlusOneTree.getAllLeaves()) {
                                        Page currentPage = new Page(file, pageNo);
                                        for (TableRecord record : currentPage.getPageRecords()) {
                                            allRowids.add(record.rowId);
                                        }
                                    }
                                }
                                //create a new index value and insert 1 index value with all rowids
                                RandomAccessFile indexFile = new RandomAccessFile(TableUtils.getIndexFilePath(table_name, columnsToUpdate.get(i)),
                                        "rw");
                                Page.addNewPage(indexFile, PageType.LEAFINDEX, -1, -1);
                                BTree bTree = new BTree(indexFile);
                                bTree.insertRow(new TableAttribute(colInfo.dataType,valueToUpdate.get(i)), allRowids);
                            }
                        }
                }
            }

            file.close();

        } catch (Exception e) {
            out.println("Unable to update the " + table_name + " file");
            out.println(e);

        }


    }

    public static void parseInsertTable(String queryString) {
        // INSERT INTO table_name ( columns ) VALUES ( values );
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
            System.out.println("! Syntax error");
            System.out.println("Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values );");

            return;
        }

        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println("! Tablename cannot be empty");
                return;
            }

            // parsing logic
            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            MetaData dstMetaData = new MetaData(tableName);

            if (!dstMetaData.tableExists) {
                System.out.println("! Table does not exist.");
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

            // Column List validation
            for (String colToken : columnTokens) {
                if (!dstMetaData.columnNames.contains(colToken.trim())) {
                    System.out.println("! Invalid column : " + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<String>(Arrays
                    .asList(valuesString.substring(valuesString.indexOf("(") + 1, valuesString.length()).split(",")));

            // fill attributes to insert
            List<TableAttribute> attributeToInsert = new ArrayList<>();

            for (ColumnInformation colInfo : dstMetaData.columnNameAttrs) {
                int i = 0;
                boolean columnProvided = false;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equals(colInfo.columnName)) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if (!colInfo.isNullable) {
                                    System.out.println("! Cannot Insert NULL into " + colInfo.columnName);
                                    return;
                                }
                                colInfo.dataType = DataTypes.NULL;
                                value = value.toUpperCase();
                            }
                            TableAttribute attr = new TableAttribute(colInfo.dataType, value);
                            attributeToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("! Invalid data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo.isNullable)
                        attributeToInsert.add(new TableAttribute(DataTypes.NULL, "NULL"));
                    else {
                        System.out.println("! Cannot Insert NULL into " + colInfo.columnName);
                        return;
                    }
                }
            }

            // insert attributes to the page
            RandomAccessFile dstTable = new RandomAccessFile(TableUtils.getTablePath(tableName), "rw");
            int dstPageNo = BPlusOneTree.getPgNoForInsert(dstTable, dstMetaData.rootPageNo);
            Page dstPage = new Page(dstTable, dstPageNo);

            int rowNo = dstPage.addTbRows(tableName, attributeToInsert);

            // update Index
            if (rowNo != -1) {

                for (int i = 0; i < dstMetaData.columnNameAttrs.size(); i++) {
                    ColumnInformation col = dstMetaData.columnNameAttrs.get(i);

                    if (col.hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(TableUtils.getIndexFilePath(tableName, col.columnName),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.insertRow(attributeToInsert.get(i), rowNo);
                    }

                }
            }

            dstTable.close();
            if (rowNo != -1)
                System.out.println("* Record Inserted");
            System.out.println();

        } catch (Exception ex) {
            System.out.println("! Error while inserting record");
            System.out.println(ex);

        }
    }

    private static void parseDeleteTable(String deleteTableString) {
        ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println("! Syntax Error");
                return;
            }

            tableName = deleteTableTokens.get(3);

            MetaData metaData = new MetaData(tableName);
            SpecialCondition condition = null;
            try {
                condition = getConditionFromQuery(metaData, deleteTableString);

            } catch (Exception e) {
                System.out.println(e);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(tableName), "rw");

            BPlusOneTree tree = new BPlusOneTree(tableFile, metaData.rootPageNo, metaData.tableName);
            List<TableRecord> deletedRecords = new ArrayList<TableRecord>();
            int count = 0;
            for (int pageNo : tree.getAllLeaves(condition)) {
                short deleteCountPerPage = 0;
                Page page = new Page(tableFile, pageNo);
                for (TableRecord record : page.getPageRecords()) {
                    if (condition != null) {
                        if (!condition.chkCondt(record.getAttributes().get(condition.columnOrdinal).fieldValue))
                            continue;
                    }

                    deletedRecords.add(record);
                    page.deleteTableRecord(tableName,
                            Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            // update Index

            // if there is no condition, all the rows will be deleted.
            // so just delete the existing index files on the table and create new ones
            if (condition == null) {
                // TODO delete exisitng index files for the table
                //and create new ones;




            } else {
                for (int i = 0; i < metaData.columnNameAttrs.size(); i++) {
                    if (metaData.columnNameAttrs.get(i).hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(TableUtils.getIndexFilePath(tableName, metaData.columnNameAttrs.get(i).columnName), "rw");
                        BTree bTree = new BTree(indexFile);
                        for (TableRecord record : deletedRecords) {
                            bTree.deleteRow(record.getAttributes().get(i),record.rowId);
                        }
                    }
                }
            }

            System.out.println();
            tableFile.close();
            System.out.println(count + " record(s) deleted!");

        } catch (Exception e) {
            System.out.println("! Error on deleting rows in table : " + tableName);
            System.out.println(e.getMessage());
        }

    }

    public static void test() {
        Scanner scan = new Scanner(System.in);
        parseUserCommand("create table test (id int, name text)");
        scan.nextLine();
        parseUserCommand("create index on test (name)");
        scan.nextLine();
        for (int i = 1; i < 35; i++)
        {
            //   System.out.println(i);
            parseUserCommand("insert into test (id , name) values (" + (i) + ", "+ i + "'arun' )");

            //scan.nextLine();
        }
        parseUserCommand("show tables");

        scan.nextLine();

    }

    private static SpecialCondition getConditionFromQuery(MetaData tableMetaData, String query) throws Exception {
        if (query.contains("where")) {
            SpecialCondition condition = new SpecialCondition(DataTypes.TEXT);
            String whereClause = query.substring(query.indexOf("where") + 6, query.length());
            ArrayList<String> whereClauseTokens = new ArrayList<String>(Arrays.asList(whereClause.split(" ")));

            // WHERE NOT column operator value
            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition.setNegation(true);
            }


            for (int i = 0; i < SpecialCondition.supportedOperators.length; i++) {
                if (whereClause.contains(SpecialCondition.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<String>(
                            Arrays.asList(whereClause.split(SpecialCondition.supportedOperators[i])));
                    {	condition.setOp(SpecialCondition.supportedOperators[i]);
                        condition.setConditionValue(whereClauseTokens.get(1).trim());
                        condition.setColumName(whereClauseTokens.get(0).trim());
                        break;
                    }

                }
            }


            if (tableMetaData.tableExists
                    && tableMetaData.columnExists(new ArrayList<String>(Arrays.asList(condition.columnName)))) {
                condition.columnOrdinal = tableMetaData.columnNames.indexOf(condition.columnName);
                condition.dataType = tableMetaData.columnNameAttrs.get(condition.columnOrdinal).dataType;
            } else {
                throw new Exception(
                        "! Invalid Table/Column : " + tableMetaData.tableName + " . " + condition.columnName);
            }
            return condition;
        } else
            return null;
    }


    public static void help() {
        out.println(TableUtils.printSeparator("*", 80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");

        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");

        out.println("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
        out.println("\tCreates a table with the given columns.\n");

        out.println("DROP TABLE <table_name>;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");

        out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
        out.println("\tInserts a new record into the table with the given values for the given columns.\n");

        out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        out.println("\tDisplay table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");

        out.println("HELP;");
        out.println("\tDisplay this help information.\n");

        out.println("EXIT;");
        out.println("\tExit the program.\n");

        out.println(TableUtils.printSeparator("*", 80));
    }

    public static void dropTable(String dropTableString) {
        System.out.println("STUB: This is the dropTable method.");
        System.out.println("\tParsing the string:\"" + dropTableString + "\"");

        String[] tokens = dropTableString.split(" ");
        if(!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println("Error");
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
        String tableName = dropTableTokens.get(2);


        parseDeleteTable("delete from table "+ DavisBaseBinaryFile.systemTablesFile + " where table_name = '"+tableName+"' ");
        parseDeleteTable("delete from table "+ DavisBaseBinaryFile.systemColumnsFile + " where table_name = '"+tableName+"' ");
        File tableFile = new File("data/"+tableName+".tbl");
        if(tableFile.delete()){
            System.out.println("table deleted");
        }else System.out.println("table doesn't exist");


        File f = new File("data/");
        File[] matchingFiles = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(tableName) && name.endsWith("ndx");
            }
        });
        boolean iFlag = false;
        for (File file : matchingFiles) {
            if(file.delete()){
                iFlag = true;
                System.out.println("index deleted");
            }
        }
        if(iFlag)
            System.out.println("drop "+tableName);
        else
            System.out.println("index doesn't exist");





        //page.DeleteTableRecord(dropTableTokens.get(1) ,record.pageHeaderIndex);
    }

    public static void parseCreateTable(String createTableString) {

        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
        // table and () check
        if (!createTableTokens.get(1).equals("table")) {
            System.out.println("! Syntax Error");
            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            System.out.println("! Tablename cannot be empty");
            return;
        }
        try {

            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<ColumnInformation> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

            short ordinalPosition = 1;

            String primaryKeyColumn = "";

            for (String columnToken : columnTokens) {

                ArrayList<String> colInfoToken = new ArrayList<String>(Arrays.asList(columnToken.trim().split(" ")));
                ColumnInformation colInfo = new ColumnInformation();
                colInfo.tableName = tableName;
                colInfo.columnName = colInfoToken.get(0);
                colInfo.isNullable = true;
                colInfo.dataType = DataTypes.get(colInfoToken.get(1).toUpperCase());
                for (int i = 0; i < colInfoToken.size(); i++) {

                    if ((colInfoToken.get(i).equals("null"))) {
                        colInfo.isNullable = true;
                    }
                    if (colInfoToken.get(i).contains("not") && (colInfoToken.get(i + 1).contains("null"))) {
                        colInfo.isNullable = false;
                        i++;
                    }

                    if ((colInfoToken.get(i).equals("unique"))) {
                        colInfo.isUnique = true;
                    } else if (colInfoToken.get(i).contains("primary") && (colInfoToken.get(i + 1).contains("key"))) {
                        colInfo.isPrimaryKey = true;
                        colInfo.isUnique = true;
                        colInfo.isNullable = false;
                        primaryKeyColumn = colInfo.columnName;
                        i++;
                    }

                }
                colInfo.ordinalPosition = ordinalPosition++;
                lstcolumnInformation.add(colInfo);

            }

            // update sys file
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    TableUtils.getTablePath(DavisBaseBinaryFile.systemTablesFile), "rw");
            MetaData davisbaseTableMetaData = new MetaData(DavisBaseBinaryFile.systemTablesFile);

            int pageNo = BPlusOneTree.getPgNoForInsert(davisbaseTablesCatalog, davisbaseTableMetaData.rootPageNo);

            Page page = new Page(davisbaseTablesCatalog, pageNo);

            int rowNo = page.addTbRows(DavisBaseBinaryFile.systemTablesFile,
                    Arrays.asList(new TableAttribute[] { new TableAttribute(DataTypes.TEXT, tableName), // DavisBaseBinaryFile.systemTablesFile->test
                            new TableAttribute(DataTypes.INT, "0"), new TableAttribute(DataTypes.SMALLINT, "0"),
                            new TableAttribute(DataTypes.SMALLINT, "0") }));
            davisbaseTablesCatalog.close();

            if (rowNo == -1) {
                System.out.println("! Duplicate table Name");
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(tableName), "rw");
            Page.addNewPage(tableFile, PageType.LEAF, -1, -1);
            tableFile.close();

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                    TableUtils.getTablePath(DavisBaseBinaryFile.systemColumnsFile), "rw");
            MetaData davisbaseColumnsMetaData = new MetaData(DavisBaseBinaryFile.systemColumnsFile);
            pageNo = BPlusOneTree.getPgNoForInsert(davisbaseColumnsCatalog, davisbaseColumnsMetaData.rootPageNo);

            Page page1 = new Page(davisbaseColumnsCatalog, pageNo);

            for (ColumnInformation column : lstcolumnInformation) {
                page1.addNewCols(column);
            }

            davisbaseColumnsCatalog.close();

            System.out.println("* Table created");

            if (primaryKeyColumn.length() > 0) {
                parseCreateIdx("create index on " + tableName + "(" + primaryKeyColumn + ")");
            }
        } catch (Exception e) {

            System.out.println("! Error on creating Table");
            System.out.println(e.getMessage());
            parseDeleteTable("delete from table " + DavisBaseBinaryFile.systemTablesFile + " where table_name = '" + tableName
                    + "' ");
            parseDeleteTable("delete from table " + DavisBaseBinaryFile.systemColumnsFile + " where table_name = '" + tableName
                    + "' ");
        }

    }

    public static void displayVersion() {
        System.out.println("SQVeryLite Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
    }
}
