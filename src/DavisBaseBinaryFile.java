import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import java.util.ArrayList;
import java.util.Arrays;
import static java.lang.System.out;
import java.util.List;
import java.util.Map;

public class DavisBaseBinaryFile {
   public static String columnsTable = "davisbase_columns";
   public static String tablesTable = "davisbase_tables";
   public static boolean showRowId = false;
   public static boolean dataStoreInitialized = false;

   RandomAccessFile file;

   public DavisBaseBinaryFile(RandomAccessFile file) {
      this.file = file;
   }

   /* This static variable controls page size. */
   static int pageSizePower = 9;
   /* the page size is always a power of 2. */
   static int pageSize = (int) Math.pow(2, pageSizePower);

   public boolean recordExists(MetaData tablemetaData, List<String> columNames, SpecialCondition condition) throws IOException{

   BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNo, tablemetaData.tableName);
   for(Integer pageNo :  bPlusOneTree.getAllLeaves(condition))
   {
         Page page = new Page(file,pageNo);
         for(TableRecord record : page.getPageRecords())
         {
            if(condition!=null)
            {
               if(!condition.chkCondt(record.getAttributes().get(condition.columnOrdinal).fieldValue))
                  continue;
            }
           return true;
         }
   }
   return false;

   }

   /**
    * This method is used to update records with list of new Values passed as an argument
    * @param tablemetaData
    * @param condition
    * @param colNames
    * @param newVal
    * @return
    * @throws IOException
    */
   public int updateRecords(MetaData tablemetaData,SpecialCondition condition, 
                  List<String> colNames, List<String> newVal) throws IOException
   {
      int count = 0;
      List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(colNames);

      //map new values to column ordinal position
      int k=0;
      Map<Integer,TableAttribute> newValueMap = new HashMap<>();

      for(String strnewValue:newVal){
           int index = ordinalPostions.get(k);

         try{
                newValueMap.put(index,
                      new TableAttribute(tablemetaData.columnNameAttrs.get(index).dataType,strnewValue));
                      }
                      catch (Exception e) {
							System.out.println("! Invalid data format for " + tablemetaData.columnNames.get(index) + " values: "
									+ strnewValue);
							return count;
						}

         k++;
      }
      BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNo,tablemetaData.tableName);
      for(Integer pageNo :  bPlusOneTree.getAllLeaves(condition))
      {
            short deleteCountPerPage = 0;
            Page page = new Page(file,pageNo);
            for(TableRecord record : page.getPageRecords())
            {
               if(condition!=null)
               {
                  if(!condition.chkCondt(record.getAttributes().get(condition.columnOrdinal).fieldValue))
                     continue;
               }
               count++;
               for(int i :newValueMap.keySet())
               {
                  TableAttribute oldValue = record.getAttributes().get(i);
                  int rowId = record.rowId;
                  if((record.getAttributes().get(i).dataType == DataTypes.TEXT
                   && record.getAttributes().get(i).fieldValue.length() == newValueMap.get(i).fieldValue.length())
                     || (record.getAttributes().get(i).dataType != DataTypes.NULL && record.getAttributes().get(i).dataType != DataTypes.TEXT)
                  ){
                     page.updateRecords(record,i,newValueMap.get(i).fieldValueByte);
                  }
                  else{
                   //Delete the record and insert a new one, update indexes
                     page.deleteTableRecord(tablemetaData.tableName ,
                     Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());
                     deleteCountPerPage++;
                     List<TableAttribute> attrs = record.getAttributes();
                     TableAttribute attr = attrs.get(i);
                     attrs.remove(i);
                     attr = newValueMap.get(i);
                     attrs.add(i, attr);
                    rowId =  page.addTbRows(tablemetaData.tableName , attrs);
                }
                
                if(tablemetaData.columnNameAttrs.get(i).hasIndex && condition!=null){
                  RandomAccessFile indexFile = new RandomAccessFile(TableUtils.getIndexFilePath(tablemetaData.columnNameAttrs.get(i).tableName, tablemetaData.columnNameAttrs.get(i).columnName), "rw");
                  BTree bTree = new BTree(indexFile);
                  bTree.deleteRow(oldValue,record.rowId);
                  bTree.insertRow(newValueMap.get(i), rowId);
                  indexFile.close();
                }
                
               }
             }
      }
    
      if(!tablemetaData.tableName.equals(tablesTable) && !tablemetaData.tableName.equals(columnsTable))
          System.out.println("* " + count+" record(s) updated.");
          
         return count;

   }

   /**
    * This static method creates the DavisBase data storage container and then
    * initializes two .tbl files to implement the two system tables,
    * davisbase_tables and davisbase_columns
    */
   public static void initializeData() {

      /** Create data directory at the current OS location to hold */
      try {
         File dataDir = new File("data");
         dataDir.mkdir();
         String[] oldTableFiles;
         oldTableFiles = dataDir.list();
         for (int i = 0; i < oldTableFiles.length; i++) {
            File anOldFile = new File(dataDir, oldTableFiles[i]);
            anOldFile.delete();
         }
      } catch (SecurityException se) {
         out.println("Unable to create data container directory");
         out.println(se);
      }

      /** Create davisbase_tables system catalog */
      try {

         int currentPageNo = 0;

         RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                 TableUtils.getTablePath(tablesTable), "rw");
         Page.addNewPage(davisbaseTablesCatalog, PageType.LEAF, -1, -1);
         Page page = new Page(davisbaseTablesCatalog,currentPageNo);

         page.addTbRows(tablesTable,Arrays.asList(new TableAttribute[] {
                 new TableAttribute(DataTypes.TEXT, DavisBaseBinaryFile.tablesTable),
                 new TableAttribute(DataTypes.INT, "2"),
                 new TableAttribute(DataTypes.SMALLINT, "0"),
                 new TableAttribute(DataTypes.SMALLINT, "0")
         }));

         page.addTbRows(tablesTable,Arrays.asList(new TableAttribute[] {
                 new TableAttribute(DataTypes.TEXT, DavisBaseBinaryFile.columnsTable),
                 new TableAttribute(DataTypes.INT, "11"),
                 new TableAttribute(DataTypes.SMALLINT, "0"),
                 new TableAttribute(DataTypes.SMALLINT, "2") }));

         davisbaseTablesCatalog.close();
      } catch (Exception e) {
         out.println("Unable to create the database_tables file");
         out.println(e);


      }

      /** Create davisbase_columns systems catalog */
      try {
         RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                 TableUtils.getTablePath(columnsTable), "rw");
         Page.addNewPage(davisbaseColumnsCatalog, PageType.LEAF, -1, -1);
         Page page = new Page(davisbaseColumnsCatalog, 0);

         short ordinal_position = 1;

         //Add new columns to davisbase_tables
         page.addNewCols(new ColumnInformation(tablesTable,DataTypes.TEXT, "table_name", true, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(tablesTable,DataTypes.INT, "record_count", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(tablesTable,DataTypes.SMALLINT, "avg_length", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(tablesTable,DataTypes.SMALLINT, "root_page", false, false, ordinal_position++));

         //Add new columns to davisbase_columns

         ordinal_position = 1;

         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.TEXT, "table_name", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.TEXT, "column_name", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.SMALLINT, "data_type", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.SMALLINT, "ordinal_position", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.TEXT, "is_nullable", false, false, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.SMALLINT, "column_key", false, true, ordinal_position++));
         page.addNewCols(new ColumnInformation(columnsTable,DataTypes.SMALLINT, "is_unique", false, false, ordinal_position++));

         davisbaseColumnsCatalog.close();
         dataStoreInitialized = true;
      } catch (Exception e) {
         out.println("Unable to create the database_columns file");
         out.println(e);
      }
   }

   /**
    * This method is used to find the root page manually
    * @param binaryFile
    * @return
    */
   public static int getRootPage(RandomAccessFile binaryFile) {
     int rootpage = 0;
      try {   
         for (int i = 0; i < binaryFile.length() / DavisBaseBinaryFile.pageSize; i++) {
            binaryFile.seek(i * DavisBaseBinaryFile.pageSize + 0x0A);
            int a =binaryFile.readInt();
          
            if (a == -1) {
               return i;
            }
         }
         return rootpage;
      } catch (Exception e) {
         out.println("error while getting root page no ");
         out.println(e);
      }
      return -1;
   }

   /**
    * This method is used to select the records from the table
    * @param tablemetaData
    * @param columNames
    * @param condition
    * @throws IOException
    */
   public void selectRecords(MetaData tablemetaData, List<String> columNames, SpecialCondition condition) throws IOException{

      //The select order might be different from the table ordinal position
      List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);
      System.out.println();
      List<Integer> printPosition = new ArrayList<>();

      int columnPrintLength = 0;
      printPosition.add(columnPrintLength);
      int totalTablePrintLength =0;
      if(showRowId)
      {
         System.out.print("rowid");
         System.out.print(TableUtils.printSeparator(" ",5));
         printPosition.add(10);
         totalTablePrintLength +=10;
      }


      for(int i:ordinalPostions)
      {
         String columnName = tablemetaData.columnNameAttrs.get(i).columnName;
         columnPrintLength = Math.max(columnName.length()
                 ,tablemetaData.columnNameAttrs.get(i).dataType.getPrintOffset()) + 5;
         printPosition.add(columnPrintLength);
         System.out.print(columnName);
         System.out.print(TableUtils.printSeparator(" ",columnPrintLength - columnName.length() ));
         totalTablePrintLength +=columnPrintLength;
      }
      System.out.println();
      System.out.println(TableUtils.printSeparator("-",totalTablePrintLength));

      BPlusOneTree bPlusOneTree = new BPlusOneTree(file, tablemetaData.rootPageNo,tablemetaData.tableName);

      String currentValue ="";
      for(Integer pageNo : bPlusOneTree.getAllLeaves(condition))
      {
         Page page = new Page(file,pageNo);
         for(TableRecord record : page.getPageRecords())
         {
            if(condition!=null)
            {
               if(!condition.chkCondt(record.getAttributes().get(condition.columnOrdinal).fieldValue))
                  continue;
            }
            int columnCount = 0;
            if(showRowId)
            {
               currentValue = Integer.valueOf(record.rowId).toString();
               System.out.print(currentValue);
               System.out.print(TableUtils.printSeparator(" ",printPosition.get(++columnCount) - currentValue.length()));
            }
            for(int i :ordinalPostions)
            {
               currentValue = record.getAttributes().get(i).fieldValue;
               System.out.print(currentValue);
               System.out.print(TableUtils.printSeparator(" ",printPosition.get(++columnCount) - currentValue.length()));
            }
            System.out.println();
         }
      }
      System.out.println();
   }
}


