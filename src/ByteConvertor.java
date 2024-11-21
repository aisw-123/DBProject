import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

//Conversion class to convert datatypes to byte and vice versa
public class ByteConvertor{

    /* To-byte functions the below functions either 
    tranforms a []byte to []Byte or vice versa*/
    public static Byte[] byteToBytes(final byte[] data){
        //get the length of the data if null return 0 else the given length
        int length = data == null ? 0 : data.length;
        //Store the answer in a result of []Byte
        Byte[] result= new Byte[length];
        //Assign the result from the []byte array to result and return the result usign for loop
        for(int i=0;i<length;i++)
            result[i] = data[i];
        return result;
    }

    public static byte[] Bytestobytes(final Byte[] data){

        if (data == null) 
        System.out.println("! Data is null");
        //get the length of the data if null return 0 else the given length

        int length = data == null ? 0 : data.length;
        //Store the answer in a result of []Byte

        byte[] result= new byte[length];
        //Assign the result from the []byte array to result and return the result usign for loop

        for(int i=0;i<length;i++)
            result[i] = data[i];
        return result;
    }

    //converts list to []bytes
    public static byte[] lsttobyteList(final List<Byte> lst){
      return Bytestobytes(lst.toArray(new Byte[lst.size()])); //converts a list to Array of Bytes using toArray 
    }

    //converts short to []Byte
    public static Byte[] shortToBytes(final short data)
    {
        //Allocates a short buffer orders it in BIG_ENDIAN converts the array cellsize to accomodate short data .
        return byteToBytes(ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array());
    }

    //convert short to []byte
    public static byte[] shortTobytes(final short data)
    {
    //Allocates a short buffer orders it in BIG_ENDIAN converts the array cellsize to accomodate short data .

        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array();
    }
    
    //convert int to []Byte
    public static Byte[] intToBytes(final int data) {
    //Allocates a integer buffer orders it in BIG_ENDIAN converts the array cellsize to accomodate int data .
		return byteToBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array());
	}

    //convert int to []byte
     public static byte[] intTobytes(final int data) {
	//Allocates a integer buffer orders it in BIG_ENDIAN converts the array cellsize to accomodate int data .

        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array();
	}
    
        //convert long to []byte
     public static byte[] longTobytes(final long data) {
	//Allocates a long buffer converts the array cellsize to accomodate long data .
		return ByteBuffer.allocate(Long.BYTES).putLong(data).array();
	}

    //convert long to []Byte
    public static Byte[] longToBytes(final long data) {
	//Allocates a long buffer converts the array cellsize to accomodate long data .
		return byteToBytes(ByteBuffer.allocate(Long.BYTES).putLong(data).array());
    }
    
    //convert float to []Byte
    public static Byte[] floatToBytes(final float data) {
	//Allocates a float buffer converts the array cellsize to accomodate float data .
		return byteToBytes(ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }
    //convert float to []byte
    public static byte[] floatTobytes(final float data) {
	//Allocates a float buffer converts the array cellsize to accomodate float data .
		return (ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }
    //convert double to []Byte
    public static Byte[] doubleToBytes(final double data) {
	//Allocates a float buffer converts the array cellsize to accomodate double data .
		return byteToBytes(ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }
    //convert double to []Byte
    public static byte[] doubleTobytes(final double data) {
	//Allocates a double buffer converts the array cellsize to accomodate double data .
		return (ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }

    /* The below functions convert the []bytes to the data type byte short int using wrapper--*/
    //convert []bytes to byte
    public static byte byteFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).get();
    }
//convert []bytes to short
    public static short shortFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }
//convert []bytes to int
    public static int intFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }
//convert []bytes to long
    public static long longFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }
//convert []bytes to float
    public static float floatFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }
//convert []bytes to double
    public static double doubleFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }
}