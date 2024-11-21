public class TableUtils {

	public static void splashScreen() {
		System.out.println(printSeparator("-",80));
		System.out.println("Welcome to MagnesiumDBLite"); // Display the string.
		System.out.println("MagnesiumDBLite Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(printSeparator("-",80));
	}

	public static String printSeparator(String s, int len) {
		String bar = "";
		for(int i = 0; i < len; i++) {
			bar += s;
		}
		return bar;
	}

	public static String getTablePath(String tableName) {
		return "data/" + tableName + ".tbl";
	}

	public static String getIndexFilePath(String tableName, String columnName) {
		return "data/" + tableName + "_" + columnName + ".ndx";
	}
}