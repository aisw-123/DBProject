import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.nio.charset.StandardCharsets;


//    TableAttribute class denotes each cell in the table (Datatypes and their values)

public class TableAttribute
{
    //represents the byte array, the format stored in binary file
    public byte[] fieldValuebyte; //Array to store byte format of data
    public Byte[] fieldValueByte; // Array to store the Byte form of the data

    public DataTypes dataType; //Refers to the DataTypes of the data as referenced by the DataTypes 
    // File in the source
    
    //converted string value of the attribute as for eg ---> value integer 1 is converted to String "1"
    public String fieldValue;

    //constructor that defines the class Attributes; which converts byte[] to string 
    TableAttribute(DataTypes dataType,byte[] fieldValue){
        this.dataType = dataType;
        this.fieldValuebyte = fieldValue;
    try{
    //The switch case searches the kind of dataType the column is of and then converts the byte array into string using ByteConverter
    //As per the requirements from the professor the dataTypes are Listed in the DataTypes file   
    switch(dataType)
    //Using getters to access private and static method from ByteConverter file
      {
         case NULL:
            this.fieldValue= "NULL"; break; //null is converted directly into "null"
            //convert []Byte to byte get the Byte value and then convert to String Do similar for converting short,long,float,double
        case TINYINT: this.fieldValue = Byte.valueOf(ByteConvertor.byteFromByteArray(fieldValuebyte)).toString(); break; 
        case SMALLINT: this.fieldValue = Short.valueOf(ByteConvertor.shortFromByteArray(fieldValuebyte)).toString(); break;
        case INT: this.fieldValue = Integer.valueOf(ByteConvertor.intFromByteArray(fieldValuebyte)).toString(); break;
        case BIGINT: this.fieldValue =  Long.valueOf(ByteConvertor.longFromByteArray(fieldValuebyte)).toString(); break;
        case FLOAT: this.fieldValue = Float.valueOf(ByteConvertor.floatFromByteArray(fieldValuebyte)).toString(); break;
        case DOUBLE: this.fieldValue = Double.valueOf(ByteConvertor.doubleFromByteArray(fieldValuebyte)).toString(); break;
        case YEAR: this.fieldValue = Integer.valueOf((int)Byte.valueOf(ByteConvertor.byteFromByteArray(fieldValuebyte))+2000).toString(); break;//Since year starts from 2000
        case TIME:
            // HH:MM:SS
            int millisSinceMidnight = ByteConvertor.intFromByteArray(fieldValuebyte) % 86400000;//as per eg given in requiremnts
            int seconds = millisSinceMidnight / 1000;
            int hours = seconds / 3600;
            int remHourSeconds = seconds % 3600;
            int minutes = remHourSeconds / 60;
            int remSeconds = remHourSeconds % 60;
            this.fieldValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
            break;
        case DATETIME:
                    //Format is YYYY-MM-DD_HH:MM:SS as per the requirements 
                    Date rawdatetime= new Date(Long.valueOf(ByteConvertor.longFromByteArray(fieldValuebyte)));
                    Calendar c =Calendar.getInstance();
                    c.setTime(rawdatetime);
                    int year= c.get(Calendar.YEAR);
                    int month= c.get(Calendar.MONTH);
                    int date= c.get(Calendar.DATE);
                    int hour= c.get(Calendar.HOUR);
                    int minute= c.get(Calendar.MINUTE);
                    int second= c.get(Calendar.SECOND);
                    this.fieldValue= String.format("%02d", year+1900) + "-" + String.format("%02d", month+1)
                    + "-" + String.format("%02d", date) + "_" + String.format("%02d", hour) + ":"
                    + String.format("%02d", minute) + ":" + String.format("%02d", second);
                    break;
        case DATE:
                    //YYYY-MM-DD
                    Date rawdate = new Date(Long.valueOf(ByteConvertor.longFromByteArray(fieldValuebyte)));
                    Calendar c1 =Calendar.getInstance();
                    c1.setTime(rawdate);
                    int y= c1.get(Calendar.YEAR);
                    int m= c1.get(Calendar.MONTH);
                    int d= c1.get(Calendar.DATE);
                    this.fieldValue = String.format("%02d",y+1900) + "-" + String.format("%02d", m+1)
                    + "-" + String.format("%02d", d);
                    break;
        case TEXT: this.fieldValue = new String(fieldValuebyte, "UTF-8"); break;//format UTF-8
         default:
         this.fieldValue= new String(fieldValuebyte, "UTF-8"); break;
      }
         this.fieldValueByte = ByteConvertor.byteToBytes(fieldValuebyte);
    } catch(Exception ex) {
        System.out.println("! Formatting exception"); //In case of formatting exception print this on console
    }

    }

    //Overloaded constructor that converts String value into a byte array
    TableAttribute(DataTypes dataType,String fieldValue) throws Exception {
        this.dataType = dataType;
        this.fieldValue = fieldValue;

        //Use switch case to convert the string value into byte array based on DataTypes

        try {
            switch(dataType)
//Using getters to access private and static method from ByteConverter file
            {
               case NULL:
                  this.fieldValuebyte = null; break;
                //Using inbuilt methods Parse float ,double int long to convert the dataTypes.
              case TINYINT: this.fieldValuebyte = new byte[]{ Byte.parseByte(fieldValue)}; break;
              case SMALLINT: this.fieldValuebyte = ByteConvertor.shortTobytes(Short.parseShort(fieldValue)); break;
              case INT: this.fieldValuebyte = ByteConvertor.intTobytes(Integer.parseInt(fieldValue)); break;
              case BIGINT: this.fieldValuebyte =  ByteConvertor.longTobytes(Long.parseLong(fieldValue)); break;
              case FLOAT: this.fieldValuebyte = ByteConvertor.floatTobytes(Float.parseFloat(fieldValue)); break;
              case DOUBLE: this.fieldValuebyte = ByteConvertor.doubleTobytes(Double.parseDouble(fieldValue)); break;
              case YEAR: this.fieldValuebyte = new byte[] { (byte) (Integer.parseInt(fieldValue) - 2000) }; break;
              case TIME: this.fieldValuebyte = ByteConvertor.intTobytes(Integer.parseInt(fieldValue)); break;
              case DATETIME:
                  SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss"); //Format given in requirement
                  Date datetime = sdftime.parse(fieldValue);  
                  this.fieldValuebyte = ByteConvertor.longTobytes(datetime.getTime());              
                break;
              case DATE:
                  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//Format given in requirement
                  Date date = sdf.parse(fieldValue);  
                  this.fieldValuebyte = ByteConvertor.longTobytes(date.getTime());              
                break;
              case TEXT: this.fieldValuebyte = fieldValue.getBytes(); break;
               default:
               this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII); break;
            }
            this.fieldValueByte = ByteConvertor.byteToBytes(fieldValuebyte);  //default unused byte 
        } catch (Exception e) {
            System.out.println("! Cannot convert " + fieldValue + " to " + dataType.toString());
            throw e;
        }
    }
   
}