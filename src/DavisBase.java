import java.io.File;
import java.util.Scanner;

public class DavisBase {
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    //Main
    public static void main(String[] args) {

        /* Display start lines */
        TableUtils.splashScreen();
        File dataDir = new File("data");

        if (!new File(dataDir, DavisBaseBinaryFile.tablesTable + ".tbl").exists()
                || !new File(dataDir, DavisBaseBinaryFile.columnsTable + ".tbl").exists())
            DavisBaseBinaryFile.initializeData();
        else
            DavisBaseBinaryFile.dataStoreInitialized = true;

        
        String userCommand = ""; //Get the user input from prompt

        while (!Settings.isExit()) {
            System.out.print(Settings.getPrompt());
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase(); //commnad is case insensitive
            
            Commands.parseUserEntry(userCommand);
        }
        System.out.println("Exiting...");
    }
}