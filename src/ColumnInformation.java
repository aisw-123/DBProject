
import java.io.File;

/* Class to denote column name and datatype  of table metadata */
public class ColumnInformation
{
    public DataTypes dataType; // data type    
    public boolean isUnique; // to check if column is unique
    public Short ordinalPosition; // add columns ordinally
    public boolean hasIndex; 
    public boolean isPrimaryKey; //to assign column as primary key
    public String columnName; // column name
    public boolean isNullable; 
    public String tableName; // table name to perfoem operation on column

    ColumnInformation(){
        
    }
    ColumnInformation(String tblName,DataTypes datatype,String clmName,boolean isUnique,boolean isNullable,short ordPosition){
        this.dataType = datatype;
        this.columnName = clmName;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.ordinalPosition = ordPosition;
        this.tableName = tblName;

        this.hasIndex = (new File(TableUtils.getIndexFilePath(tableName, clmName)).exists());

    }

    public void setAsPrimaryKey(){
        isPrimaryKey = true;
    }
}