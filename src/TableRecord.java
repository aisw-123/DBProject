import java.util.List;

import java.util.Arrays;
import java.util.ArrayList;

//This class contains helper methods for converting the table record data (header and body) into byte array
public class TableRecord
{
    public int rowId; //row id of unique row
    public Byte[] recordBody; //body
    public short recordOffset; // record off set
    public Byte[] colDatatypes; // column data type
    public short pageHeaderIndex; // page header index
    private List<TableAttribute> attributes; // attributes

    // Table attribute to define record
    TableRecord(short pageIndex,int rowId, short recordoffset, byte[] columnDatatypes, byte[] recordBody)
    {
        this.rowId = rowId;
        this.recordBody= ByteConvertor.byteToBytes(recordBody);
        this.colDatatypes = ByteConvertor.byteToBytes(columnDatatypes);
        this.recordOffset =  recordoffset;
        this.pageHeaderIndex = pageIndex;
        setTableAttributes();
    }

    // set values in record
    private void setTableAttributes()
    {
        attributes = new ArrayList<>();
        int pointer = 0;
        for(Byte dataType : colDatatypes)
        {
             byte[] fieldValue = ByteConvertor.Bytestobytes(Arrays.copyOfRange(recordBody,pointer, pointer + DataTypes.getLength(dataType)));
             attributes.add(new TableAttribute(DataTypes.get(dataType), fieldValue));
                    pointer =  pointer + DataTypes.getLength(dataType);
        }
    }

    // return table attribute 
     public List<TableAttribute> getAttributes()
    {
        return attributes;
    }    
}