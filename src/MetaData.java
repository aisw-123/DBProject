/* This class is used to read and change the table's meta data (davisbase tables and davisbas columns).
 We must ensure that the meta data 
 - Record count 
 - root page is updated. 
 When a Record is inserted or deleted, the table's number is incremented.
 */


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;

 
public class MetaData{

    public int recordCount; //total Record
    public List<TableRecord> columnData; //column data for table (Table Record object)
    public List<ColumnInformation> columnNameAttrs; //column name attributes
    public List<String> columnNames; //column name
    public String tableName; //table name
    public boolean tableExists; // to verfy if table exists or not
    public int rootPageNo; //root page number
    public int lastRowId; // last row id of table


    public MetaData(String tableName)
    {
        this.tableName = tableName;
        tableExists = false;
        try {

            RandomAccessFile dbtableCatelog = new RandomAccessFile(
            TableUtils.getTablePath(DavisBaseBinaryFile.tablesTable), "r");
            
            //get the root page of the table
            int rootPageNo = DavisBaseBinaryFile.getRootPage(dbtableCatelog);
           
            BPlusOneTree bplusOneTree = new BPlusOneTree(dbtableCatelog, rootPageNo,tableName);
            //search through all leaf papges in davisbase_tables
            for (Integer pageNo : bplusOneTree.getAllLeaves()) {
               Page page = new Page(dbtableCatelog, pageNo);
               //search theough all the records in each page
               for (TableRecord Record : page.getPageRecords()) {
                   //if the Record with table is found, get the root page No and Record count; break the loop
                  if (new String(Record.getAttributes().get(0).fieldValue).equals(tableName)) {
                    this.rootPageNo = Integer.parseInt(Record.getAttributes().get(3).fieldValue);
                    recordCount = Integer.parseInt(Record.getAttributes().get(1).fieldValue);
                    tableExists = true;
                     break;
                  }
               }
               if(tableExists)
                break;
            }
   
            dbtableCatelog.close();
            if(tableExists)
            {
               loadColumnData();
            } else {
               throw new Exception("Table does not exist.");
            }
            
         } catch (Exception e) {
           // System.out.println("! Error while checking Table " + tableName + " exists.");
            //debug: System.out.println(e);
         }
    }

    public List<Integer> getOrdinalPostions(List<String> columns){
				List<Integer> ordPostions = new ArrayList<>();
				for(String column :columns)
				{
					ordPostions.add(columnNames.indexOf(column));
                }
                return ordPostions;
    }

    //loads the column information for thr table
    private void loadColumnData() {
        try {
  
           RandomAccessFile dbColumnsCatalog = new RandomAccessFile(
            TableUtils.getTablePath(DavisBaseBinaryFile.columnsTable), "r");
           int rootPageNo = DavisBaseBinaryFile.getRootPage(dbColumnsCatalog);
  
           columnData = new ArrayList<>();
           columnNameAttrs = new ArrayList<>();
           columnNames = new ArrayList<>();
           BPlusOneTree bPlusOneTree = new BPlusOneTree(dbColumnsCatalog, rootPageNo,tableName);
         
           /* Get all columns from the davisbase_columns, loop through all the leaf pages 
           and find the records with the table name */
           for (Integer pageNo : bPlusOneTree.getAllLeaves()) {
           
             Page page = new Page(dbColumnsCatalog, pageNo);
              
              for (TableRecord Record : page.getPageRecords()) {
                  
                 if (Record.getAttributes().get(0).fieldValue.equals(tableName)) {
                    {
                     //set column information in the data members of the class
                       columnData.add(Record);
                       columnNames.add(Record.getAttributes().get(1).fieldValue);
                       ColumnInformation columnInfo = new ColumnInformation(
                                          tableName  
                                        , DataTypes.get(Record.getAttributes().get(2).fieldValue)
                                        , Record.getAttributes().get(1).fieldValue
                                        , Record.getAttributes().get(6).fieldValue.equals("YES")
                                        , Record.getAttributes().get(4).fieldValue.equals("YES")
                                        , Short.parseShort(Record.getAttributes().get(3).fieldValue)
                                        );
                                          
                    if(Record.getAttributes().get(5).fieldValue.equals("PRI"))
                          columnInfo.setAsPrimaryKey();
                        
                     columnNameAttrs.add(columnInfo);                      
                    }
                 }
              }
           }
  
           dbColumnsCatalog.close();
        } catch (Exception e) {
           System.out.println("! Error while getting column data for " + tableName);
        }
     }

     // Method to check if the columns exists for the table
   public boolean columnExists(List<String> columns) {

   // return true if column does not exists
    if(columns.size() == 0)
       return true;       

     List<String> lColumns =new ArrayList<>(columns);

      for (ColumnInformation column_name_attr : columnNameAttrs) {
         if (lColumns.contains(column_name_attr.columnName))
            lColumns.remove(column_name_attr.columnName);
      }

      return lColumns.isEmpty();
   }    

// Update table data
 public void updateMetaData()
 {

   //update root page in the tables catalog
   try{
         RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(tableName), "r");
   
         Integer rootPageNo = DavisBaseBinaryFile.getRootPage(tableFile);
         tableFile.close();
         // initialise davisbase catelog                   
         RandomAccessFile dbtableCatelog = new RandomAccessFile(TableUtils.getTablePath(DavisBaseBinaryFile.tablesTable), "rw");       
         DavisBaseBinaryFile tablesBinaryFile = new DavisBaseBinaryFile(dbtableCatelog);
         MetaData tablesMetaData = new MetaData(DavisBaseBinaryFile.tablesTable);         
         SpecialCondition cdtn = new SpecialCondition(DataTypes.TEXT);
         cdtn.setColumName("table_name");
         cdtn.columnOrdinal = 0;
         cdtn.setConditionValue(tableName);
         cdtn.setOp("=");

         List<String> columns = Arrays.asList("record_count","root_page");
         List<String> newValues = new ArrayList<>();

         newValues.add(new Integer(recordCount).toString());
         newValues.add(new Integer(rootPageNo).toString());

         tablesBinaryFile.updateRecords(tablesMetaData,cdtn,columns,newValues);                                              
         dbtableCatelog.close();
   }
   catch(IOException e){
      System.out.println("! Error updating meta data for " + tableName);
   }   
 }

// validate column before adding whether that column exists or not
 public boolean validateInsert(List<TableAttribute> row) throws IOException
 {
   RandomAccessFile tableFile = new RandomAccessFile(TableUtils.getTablePath(tableName), "r");
   DavisBaseBinaryFile file = new DavisBaseBinaryFile(tableFile);                  
      for(int i=0;i<columnNameAttrs.size();i++)
      {      
         SpecialCondition cdtn = new SpecialCondition(columnNameAttrs.get(i).dataType);
         cdtn.columnName = columnNameAttrs.get(i).columnName;
         cdtn.columnOrdinal = i;
         cdtn.setOp("=");

         if(columnNameAttrs.get(i).isUnique)
         {
               cdtn.setConditionValue(row.get(i).fieldValue);
               
               if(file.recordExists(this, Arrays.asList(columnNameAttrs.get(i).columnName), cdtn)){
                  // trying to add column name that already exists
                  System.out.println("! Insert failed: Column "+ columnNameAttrs.get(i).columnName + " should be unique." );
                  tableFile.close();
                  return false;
               }      
            }
         }
         tableFile.close();
         return true;
      }
   }