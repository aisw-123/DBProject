import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.nio.charset.StandardCharsets;

public class TableAttribute {
    public byte[] fldValbyte; //Stores byte form of the data
    public Byte[] fldValByte; // Stores Byte form of the data
    public DataTypes dt; //Refers to the DataTypes of the data as referenced by the DataTypes 
    public String fldVal; // converts string val of the attribute


    TableAttribute(DataTypes dataType, byte[] fieldValue){
        this.dt = dataType;
        this.fldValbyte = fieldValue;
        try{
            if (dt == DataTypes.NULL) {
                this.fldVal = "NULL";
            } else if (dt == DataTypes.TINYINT) {
                this.fldVal = Byte.valueOf(ByteConvertor.byteFromByteArray(fldValbyte)).toString();
            } else if (dt == DataTypes.SMALLINT) {
                this.fldVal = Short.valueOf(ByteConvertor.shortFromByteArray(fldValbyte)).toString();
            } else if (dt == DataTypes.INT) {
                this.fldVal = Integer.valueOf(ByteConvertor.intFromByteArray(fldValbyte)).toString();
            } else if (dt == DataTypes.BIGINT) {
                this.fldVal =  Long.valueOf(ByteConvertor.longFromByteArray(fldValbyte)).toString();
            } else if (dt == DataTypes.FLOAT) {
                this.fldVal = Float.valueOf(ByteConvertor.floatFromByteArray(fldValbyte)).toString();
            } else if (dt == DataTypes.DOUBLE) {
                this.fldVal = Double.valueOf(ByteConvertor.doubleFromByteArray(fldValbyte)).toString(); 
            } else if (dt == DataTypes.YEAR) {
                this.fldVal = Integer.valueOf((int)Byte.valueOf(ByteConvertor.byteFromByteArray(fldValbyte))+2000).toString();
            } else if (dt == DataTypes.TIME) {
                int millisSinceMidnight = ByteConvertor.intFromByteArray(fldValbyte) % 86400000;//as per eg given in requiremnts
                int seconds = millisSinceMidnight / 1000;
                int hours = seconds / 3600;
                int remHourSeconds = seconds % 3600;
                int minutes = remHourSeconds / 60;
                int remSeconds = remHourSeconds % 60;
                this.fldVal = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
            } else if (dt == DataTypes.DATETIME) {
                Date rawdatetime= new Date(Long.valueOf(ByteConvertor.longFromByteArray(fldValbyte)));
                Calendar c =Calendar.getInstance();
                c.setTime(rawdatetime);
                int year= c.get(Calendar.YEAR);
                int month= c.get(Calendar.MONTH);
                int date= c.get(Calendar.DATE);
                int hour= c.get(Calendar.HOUR);
                int minute= c.get(Calendar.MINUTE);
                int second= c.get(Calendar.SECOND);
                this.fldVal= String.format("%02d", year+1900) + "-" + String.format("%02d", month+1)
                + "-" + String.format("%02d", date) + "_" + String.format("%02d", hour) + ":"
                + String.format("%02d", minute) + ":" + String.format("%02d", second);
            } else if (dt == DataTypes.DATE) {
                Date rawdate = new Date(Long.valueOf(ByteConvertor.longFromByteArray(fldValbyte)));
                Calendar c1 =Calendar.getInstance();
                c1.setTime(rawdate);
                int y= c1.get(Calendar.YEAR);
                int m= c1.get(Calendar.MONTH);
                int d= c1.get(Calendar.DATE);
                this.fldVal = String.format("%02d",y+1900) + "-" + String.format("%02d", m+1) + "-" + String.format("%02d", d);
            } else if (dt == DataTypes.TEXT) {
                this.fldVal = new String(fldValbyte, "UTF-8");
            } else {
                this.fldVal= new String(fldValbyte, "UTF-8");
            }
            this.fldValByte = ByteConvertor.byteToBytes(fldValbyte);
            
        } catch(Exception ex) {
            System.out.println("! Formatting exception"); 
        }
    }  

    //Converts str val to byte array
    TableAttribute(DataTypes dataType,String fieldValue) throws Exception {
        this.dt = dataType;
        this.fldVal = fieldValue;

        try {
            if(dt == DataTypes.NULL) {
                this.fldValbyte = null ;
            } else if (dt == DataTypes.TINYINT) {
                this.fldValbyte = new byte[]{Byte.parseByte(fldVal)};
            } else if (dt == DataTypes.SMALLINT) {
                this.fldValbyte = ByteConvertor.shortTobytes(Short.parseShort(fldVal));
            } else if (dt == DataTypes.INT) {
                this.fldValbyte = ByteConvertor.intTobytes(Integer.parseInt(fldVal));
            } else if (dt == DataTypes.BIGINT) {
                this.fldValbyte =  ByteConvertor.longTobytes(Long.parseLong(fldVal)); 
            } else if (dt == DataTypes.FLOAT) {
                this.fldValbyte = ByteConvertor.floatTobytes(Float.parseFloat(fldVal));
            } else if (dt == DataTypes.DOUBLE) {
                this.fldValbyte = ByteConvertor.doubleTobytes(Double.parseDouble(fldVal));
            } else if (dt == DataTypes.YEAR) {
                this.fldValbyte = new byte[] { (byte) (Integer.parseInt(fldVal) - 2000) }; 
            } else if (dt == DataTypes.TIME) {
                this.fldValbyte = ByteConvertor.intTobytes(Integer.parseInt(fldVal));
            } else if (dt == DataTypes.DATETIME) {
                SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss"); 
                Date datetime = sdftime.parse(fldVal);  
                this.fldValbyte = ByteConvertor.longTobytes(datetime.getTime()); 
            } else if (dt == DataTypes.DATE) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sdf.parse(fldVal);  
                this.fldValbyte = ByteConvertor.longTobytes(date.getTime());
            } else if (dt == DataTypes.TEXT) {
                this.fldValbyte = fldVal.getBytes();
            } else {
                this.fldValbyte = fldVal.getBytes(StandardCharsets.US_ASCII);
            }
            
            this.fldValByte = ByteConvertor.byteToBytes(fldValbyte); 

        } catch (Exception e) {
            System.out.println("! Cannot convert " + fldVal + " to " + dt.toString());
            throw e;
        }
    }
}