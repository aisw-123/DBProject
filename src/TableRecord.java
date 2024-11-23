import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;


public class TableRecord {
    
    public int rId; //row id 
    public Byte[] recBody; //body for records
    public short recOffst; // record offset
    public Byte[] colDt; // column data type
    public short pgHeaderIndx; // page header index
    private List<TableAttribute> attr; // attributes


    TableRecord(short pageIndex,int rowId, short recordoffset, byte[] columnDatatypes, byte[] recordBody) {
        this.rId = rowId;
        this.recBody= ByteConvertor.byteToBytes(recordBody);
        this.colDt = ByteConvertor.byteToBytes(columnDatatypes);
        this.recOffst =  recordoffset;
        this.pgHeaderIndx = pageIndex;
        setTableAttributes();
    }

    private void setTableAttributes() {
        attr = new ArrayList<>();
        int pntr = 0;
        for(Byte dataType : colDt) {
            byte[] fieldValue = ByteConvertor.Bytestobytes(Arrays.copyOfRange(recBody,pntr, pntr + DataTypes.getLength(dataType)));
            attr.add(new TableAttribute(DataTypes.get(dataType), fieldValue));
                    pntr =  pntr + DataTypes.getLength(dataType);
        }
    }

    public List<TableAttribute> getAttributes() {
        return attr;
    }    
}