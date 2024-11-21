import java.util.HashMap;
import java.util.Map;

public enum PageType {
    INTERIOR((byte)5),
    INTERIORINDEX((byte)2),
    LEAF((byte)13),
    LEAFINDEX((byte)10);
    
 // hash map is used to store the various page types  
 private static final Map<Byte,PageType> pgType = new HashMap<Byte,PageType>();

 public byte getVal() { return value; }
    
 static {
      for(PageType s : PageType.values())
      pgType.put(s.getVal(), s);
 }
 
 private byte value;

 PageType(byte value) {
      this.value = value;
 }
 
 //return the page type
 public static PageType get(byte value) { 
      return pgType.get(value); 
 }
 
}