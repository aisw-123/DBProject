public class TableUtils {

	public static void splashScreen() {

		// Print out the entry screen
		System.out.println(printSeparator("-",80));
		System.out.println("Welcome to MagnesiumDBLite"); 
		System.out.println("MagnesiumDBLite Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
		System.out.println("\nType \"help;\" to show the list of available commands.");
		System.out.println(printSeparator("-",80));
	}

	public static String printSeparator(String c, int len) {
		StringBuilder sep = new StringBuilder(len);
		for(int i = 0; i < len; i++) {
			sep.append(c);
		}
		return sep.toString();
	}

	public static String getTablePath(String tblName) {
		return String.format("data/%s.tbl", tblName);
	}

	public static String getIndexFilePath(String tblName, String colName) {
		return String.format("data/%s_%s.ndx",tblName, colName);
	}
}