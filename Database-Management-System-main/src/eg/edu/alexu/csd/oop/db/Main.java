package eg.edu.alexu.csd.oop.db;

import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    private static final Scanner scanner = new Scanner(System.in);
    public static void main(String[] args) throws SQLException {
        Parser parser = new Parser();
        String query="";
        while (!query.toLowerCase().equals(".quit")){
            query = scanner.nextLine();
            if(!query.toLowerCase().equals(".quit")) {
                parser.whatCommand(query);
            }
        }
        scanner.close();
    }
}