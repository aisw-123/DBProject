import java.io.File;
import java.util.Scanner;

public class DavisBase { 

    static Scanner commandInputScanner = new Scanner(System.in).useDelimiter(";"); // Renamed for clarity

    public static void main(String[] args) {

        // Display the welcome screen
        TableUtils.displayWelcomeScreen(); // Renamed splashScreen() to displayWelcomeScreen

        File dataDirectory = new File("data"); // Renamed dataDir to dataDirectory

        // Check if system tables exist, and initialize if they don't
        if (!new File(dataDirectory, DavisBaseBinaryFile.systemTablesFile + ".tbl").exists()
                || !new File(dataDirectory, DavisBaseBinaryFile.systemColumnsFile + ".tbl").exists()) {
            DavisBaseBinaryFile.initializeSystemTables(); // Renamed initializeData() to initializeSystemTables()
        } else {
            DavisBaseBinaryFile.isSystemInitialized = true; // Renamed dataStoreInitialized to isSystemInitialized
        }

        String userCommand = ""; // Renamed userCommand to make intent explicit

        // Main loop to process user commands
        while (!Settings.isExitRequested()) { // Renamed isExit() to isExitRequested()
            System.out.print(Settings.getPromptMessage()); // Renamed getPrompt() to getPromptMessage()
            userCommand = commandInputScanner.next()
                .replace("\n", " ")
                .replace("\r", "")
                .trim()
                .toLowerCase(); // Normalize input for consistent processing

            Commands.parseUserCommand(userCommand); // Renamed parseUserEntry() to parseUserCommand()
        }

        System.out.println("Exiting DavisBase...");
    }
}
