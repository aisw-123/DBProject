import java.util.HashMap;
import java.util.Map;

public enum DataTypes {        //class to get datatype based on bytes
     NULL((byte)0){ 
          @Override
          public String toString(){ return "NULL"; }},
     TINYINT((byte)1){ 
      @Override
      public String toString(){ return "TINYINT"; }},
     SMALLINT((byte)2){ 
      @Override
      public String toString(){ return "SMALLINT"; }},
     INT((byte)3){ 
      @Override
      public String toString(){ return "INT"; }},
     BIGINT((byte)4){ 
      @Override
      public String toString(){ return "BIGINT"; }},
     FLOAT((byte)5){ 
      @Override
      public String toString(){ return "FLOAT"; }},
     DOUBLE((byte)6){ 
      @Override
      public String toString(){ return "DOUBLE"; }},
     YEAR((byte)8){ 
      @Override
      public String toString(){ return "YEAR"; }},
     TIME((byte)9){ 
      @Override
      public String toString(){ return "TIME"; }},
     DATETIME((byte)10){ 
      @Override
      public String toString(){ return "DATETIME"; }},
     DATE((byte)11){ 
      @Override
      public String toString(){ return "DATE"; }},
     TEXT((byte)12){ 
      @Override
      public String toString(){ return "TEXT"; }};
     
    
     
 private static final Map<Byte,DataTypes> dataTypeLookup = new HashMap<Byte,DataTypes>();
 private static final Map<Byte,Integer> dataTypeSizeLookup = new HashMap<Byte,Integer>();
 private static final Map<String,DataTypes> dataTypeStringLookup = new HashMap<String,DataTypes>();
 private static final Map<DataTypes,Integer> dataTypePrintOffset = new HashMap<DataTypes,Integer>();



 static {
      for(DataTypes s : DataTypes.values())
          {
               dataTypeLookup.put(s.getVal(), s);
               dataTypeStringLookup.put(s.toString(), s);
              
               if(s == DataTypes.TINYINT || s== DataTypes.YEAR)
                   {
                          dataTypeSizeLookup.put(s.getVal(), 1);
                          dataTypePrintOffset.put(s, 6);
                   }
               else if(s == DataTypes.SMALLINT){
                    dataTypeSizeLookup.put(s.getVal(), 2);
                    dataTypePrintOffset.put(s, 8);
               }
               else if(s == DataTypes.INT || s == DataTypes.FLOAT || s == DataTypes.TIME){
                    dataTypeSizeLookup.put(s.getVal(), 4);
                    dataTypePrintOffset.put(s, 10);
               }
               else if(s == DataTypes.BIGINT || s == DataTypes.DOUBLE
                          || s == DataTypes.DATETIME || s == DataTypes.DATE ){
                    dataTypeSizeLookup.put(s.getVal(), 8);
                    dataTypePrintOffset.put(s, 25);
                          }
               else if(s == DataTypes.TEXT){
                    dataTypePrintOffset.put(s, 25);
               }
               else if(s == DataTypes.NULL){
                    dataTypeSizeLookup.put(s.getVal(), 0);
                    dataTypePrintOffset.put(s, 6);
               }
          }


 }
 private byte value;

 private DataTypes(byte val) {
      this.value = val;
 }

 

 public byte getVal() { return value; }

 public static DataTypes get(byte val) { 
     if(val > 12)
        return DataTypes.TEXT;
      return dataTypeLookup.get(val); 
 }

 //Get datatype from String map 
 public static DataTypes get(String txt) { 
      return dataTypeStringLookup.get(txt); 
 }

 public static int getLength(DataTypes type){
     return getLength(type.getVal());
}
 public static int getLength(byte val){
     if(get(val)!=DataTypes.TEXT)
          return dataTypeSizeLookup.get(val);
     else
          return val - 12;
 }

 public int getPrintOffset(){
      return dataTypePrintOffset.get(get(this.value));
 }

 
}