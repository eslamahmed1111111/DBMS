package eg.edu.alexu.csd.oop.db;

import java.sql.SQLException;

public class Parser {

    private  DBMS_N database;
    private Table table;
    public Parser(String databaseNameTest){
        database = new DBMS_N(databaseNameTest);
    }
    public Parser(){}
    public void whatCommand(String query) throws SQLException {
        String commandTemp;
        if(query.matches("(?i)^([ ]*create[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
//            database.setQuery(query);
            database = new DBMS_N();
            database.executeStructureQuery(query.toLowerCase());

        }else if(query.matches("(?i)^([ ]*create[ ]+table[ ]+[a-z_0-9]+" +
                "[ ]*[(][ ]*([a-z_0-9]+[ ]+[a-z_0-9]+[ ]*,[ ]*)*([a-z_0-9]+[ ]+[a-z_0-9]+)+[ ]*[)][ ]*)$")){
//                database.setQuery(query);
            database.executeStructureQuery(query.toLowerCase());

        }else if(query.matches("(?i)^([ ]*drop[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
//            database.setQuery(query);
            database.executeStructureQuery(query.toLowerCase());
        }else if(query.matches("(?i)^([ ]*drop[ ]+table[ ]+[a-z_0-9]+[ ]*)$")){
//            database.setQuery(query);
            database.executeStructureQuery(query.toLowerCase());

        }else if(query.matches("(?i)^([ ]*insert[ ]+into[ ]+[a-z_0-9]+[ ]*([(].*[)])*[ ]*values[ ]*[(].*[)][ ]*)$")){
            database.setQuery();
            database.executeUpdateQuery(query);
        }
        else if(query.matches("(?i)^([ ]*select[ ]+(([a-z_0-9]+[ ]*,[ ]*)*([a-z_0-9]+)+|\\*)[ ]+from[ ]+[a-z_0-9]+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            database.setQuery();
            database.executeQuery(query);
        }else if(query.matches("(?i)^([ ]*delete[ ]+from[ ]+[a-z_0-9]+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            database.setQuery();
            database.executeUpdateQuery(query);
        }else if(query.matches("(?i)^([ ]*update[ ]+[a-z_0-9]+[ ]+" +
                "set[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*,[ ]*)*([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+)+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            database.setQuery();
            database.executeUpdateQuery(query);
        }else{
            System.out.println("syntax error");
        }
    }

    public boolean testWrongQuery(String query){
        /*
         *if integer or string
         * validate " "
         * check other tests
         * rage3 el regex hate3raf eh elly na2es
         * 2abl el kaws wel3alama wel 7agat dy space or not..check
         */
        if(query.matches("(?i)^([ ]*create[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
            return true;
        }else if(query.matches("(?i)^([ ]*create[ ]+table[ ]+[a-z_0-9]+" +
                "[ ]*[(][ ]*([a-z_0-9]+[ ]+[a-z_0-9]+[ ]*,[ ]*)*([a-z_0-9]+[ ]+[a-z_0-9]+)+[ ]*[)][ ]*)$")){
            return true;
        }else if(query.matches("(?i)^([ ]*drop[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
            return true;

        }else if(query.matches("(?i)^([ ]*drop[ ]+table[ ]+[a-z_0-9]+[ ]*)$")){
            return true;

        }else if(query.matches("(?i)^([ ]*insert[ ]+into[ ]+[a-z_0-9]+[ ]*([(].*[)])*[ ]*values[ ]*[(].*[)][ ]*)$")){
            return true;

        }
        else if(query.matches("(?i)^([ ]*select[ ]+(([a-z_0-9]+[ ]*,[ ]*)*([a-z_0-9]+)+|\\*)[ ]+from[ ]+[a-z_0-9]+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            return true;

        }else if(query.matches("(?i)^([ ]*delete[ ]+from[ ]+[a-z_0-9]+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            return true;

        }else if(query.matches("(?i)^([ ]*update[ ]+[a-z_0-9]+[ ]+" +
                "set[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*,[ ]*)*([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+)+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")){
            return true;

        }
        return false;
    }

}
