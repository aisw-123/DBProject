
import java.io.File;

/* Class to denote column name and datatype  of table metadata */
public class ColumnInformation
{
    public DataTypes dType; // data type    
    public boolean unique; // to check if column is unique
    public Short ordPos; // add columns ordinally
    public boolean hasIdx; 
    public boolean isPrimKey; //to assign column as primary key
    public String colName; // column name
    public boolean isNullable; 
    public String tblName; // table name to perfoem operation on column

    ColumnInformation(){
        
    }
    ColumnInformation(String tblName,DataTypes datatype,String clmName,boolean unique,boolean isNullable,short ordPosition){
        this.dType = datatype;
        this.colName = clmName;
        this.unique = unique;
        this.isNullable = isNullable;
        this.ordPos = ordPosition;
        this.tblName = tblName;

        this.hasIdx = (new File(TableUtils.getIndexFilePath(clmName)).exists());

    }

    public void setAsPrimaryKey(){
        isPrimKey = true;
    }
}
