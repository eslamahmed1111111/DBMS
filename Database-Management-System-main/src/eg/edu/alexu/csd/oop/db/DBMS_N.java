package eg.edu.alexu.csd.oop.db;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class DBMS_N implements Database {
    private String databaseName;
    private String [] arr ;
    private Table table;
    private String[] command;
    private Parser parser;


    public DBMS_N() {
        parser = new Parser();
    }

    public  DBMS_N( String databaseName){
        this.databaseName = databaseName.toLowerCase();
        parser = new Parser();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setQuery() {
    }
    @Override
    public String createDatabase(String databaseName, boolean dropIfExists) {
        databaseName = databaseName.toLowerCase();
            if (dropIfExists) {
                File file = new File(databaseName.toLowerCase());
                File[] files = file.listFiles();
                for (File f : files != null ? files : new File[0]) {
                    f.delete();
                }

            }
            //TO Lower??
            File file = new File(databaseName.toLowerCase());
            System.getProperty("databaseName.separator");
            file.mkdirs();

        return this.databaseName=file.getPath();
//            return databaseName;
    }
    public void dropDatabase(String databaseName, boolean dropIfExists){
        databaseName = databaseName.toLowerCase();
            if (dropIfExists) {
                File file = new File(databaseName);
                File[] files = file.listFiles();
                for (File f : files) {
                    f.delete();
                }
                file.delete();
            }
    }


    @Override
    public boolean executeStructureQuery(String query) throws java.sql.SQLException {
        query =query.toLowerCase();
//        parser =new Parser("TestDB");
        String commandTemp;
        if(parser.testWrongQuery(query)){
            if(query.matches("(?i)^([ ]*create[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
                commandTemp = query.substring(query.toLowerCase().indexOf("database"));
                this.command =commandTemp.split("[ ]+");
                try {
                    createDatabase(this.command[1] , dropIfExists(this.command[1]));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return true;
            }else if(query.matches("(?i)^([ ]*create[ ]+table[ ]+[a-z_0-9]+" +
                    "[ ]*[(][ ]*([a-z_0-9]+[ ]+[a-z_0-9]+[ ]*,[ ]*)*([a-z_0-9]+[ ]+[a-z_0-9]+)+[ ]*[)][ ]*)$")){
                commandTemp = query.substring(query.toLowerCase().indexOf("table"),query.toLowerCase().indexOf("("));
                this.command =commandTemp.split("[ ]+");
                String columns = query.substring(query.indexOf("("), query.indexOf(")"));//kda el kawas ela5eer me4 ma3aya
                table = new Table(getDatabaseName(), this.command[1], columns);
                try {
                    return table.createTable(table.dropIfExists(getDatabaseName()+"\\"+this.command[1]+".xml"));
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }else if(query.matches("(?i)^([ ]*drop[ ]+database[ ]+[a-z_0-9]+[ ]*)$")){
                commandTemp = query.substring(query.toLowerCase().lastIndexOf("database"));
                this.command =commandTemp.split("[ ]+");
                try {
                    dropDatabase(this.command[1] , dropIfExists(this.command[1]));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return true;
            }else if(query.matches("(?i)^([ ]*drop[ ]+table[ ]+[a-z_0-9]+[ ]*)$")) {
                commandTemp = query.substring(query.toLowerCase().indexOf("table"));
                this.command =commandTemp.split("[ ]+");
                try {
                    table.dropTable(table.dropIfExists(getDatabaseName()+"\\"+this.command[1]+".xml"));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return true;
            }
        }
        throw new SQLException();
    }






    @Override
    public Object[][] executeQuery(String query) throws SQLException {
        int pivot = 0;
        String word = "from";
        String word2 = "where";
        String jj="select" ;
        String w = " ";
        String colname =" " ;
        String  zz= "'" ;
        String ope = " ";
        String re;
        String we ;
        int limit = 0;
        String f=" " ;
        int c=0;
        String[] aa = query.split("[ ]+");

        String[] arr2;
        String[] arr4 ;

        if (aa[1].equals("*")) {
            if (!query.toLowerCase().contains(word2)) {
                pivot = 1;
                w = aa[3];
            } else {

                pivot = 2;
                colname=aa[5] ;
                w = aa[3];
                ope=aa[6] ;
                if(!aa[7].contains(zz)) {
                    limit = Integer.parseInt(aa[7]);
                    c=1;
                }else if(aa[7].contains(zz)&&(ope.equals("="))||(ope.equals(("!=")))){

                    f=aa[7];
                    c=2;
                }else {
                    throw new SQLException () ;
                }

            }
        } else {
            if (!query.toLowerCase().contains(word2)) {
                re = query.substring(query.indexOf(" ")+1, query.toLowerCase().indexOf(word.toLowerCase()));
                query = query.substring(query.toLowerCase().indexOf(word.toLowerCase()));
                this.arr = re.split("[, ]+");
                arr2 = query.split("[ ]+");
                w = arr2[1];
                pivot = 3;
            }else{
                pivot = 4;
                re = query.substring(query.indexOf(" ")+1, query.toLowerCase().indexOf(word.toLowerCase()));
                we = query.substring(query.toLowerCase().indexOf(word.toLowerCase()),query.toLowerCase().indexOf(word2));
                query=query.substring(query.toLowerCase().indexOf(word2));
                this.arr = re.split("[, ]+");
                arr2 = we.split("[ ]+");
                w = arr2[1];
                arr4=query.split("[ ]+") ;
                colname=arr4[1] ;
                ope=arr4[2] ;
                if(!arr4[3].contains(zz)) {
                    limit = Integer.parseInt(arr4[3]);
                    c=1;
                }else if(arr4[3].contains(zz)&&(ope.equals("="))||(ope.equals(("!=")))){

                    f=arr4[3];
                    c=2;
                }else {
                    throw new SQLException () ;
                }






            }

        }

        File file = new File(databaseName + "\\" + w + ".xml");
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        DocumentBuilder builder = null;
        try {
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        Document document = null;
        try {
            document = builder.parse(file);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        NodeList list = document.getElementsByTagName("row");
        int nocol = 0;
        ArrayList<Object> ii = new ArrayList() ;
        Node q = list.item(0);
        if (q.getNodeType() == Node.ELEMENT_NODE) {
            Element element = (Element) q;
            NodeList names = element.getChildNodes();
            for (int j = 0; j < names.getLength(); j++) {
                Node r = names.item(j);
                if (r.getNodeType() == Node.ELEMENT_NODE) {
                    Element name = (Element) r;
                    nocol++;
                    ii.add(name.getTagName()) ;


                }
            }
        }
        Object[][] arr1 = new Object[list.getLength()+1][nocol];
        for(int j =0 ;j<nocol;j++){

            arr1[0][j]=ii.get(j) ;
        }
        int count = 0;


        for (int i = 1; i < list.getLength()+1; i++) {
            count = 0;
            Node node = list.item(i-1);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                NodeList names = element.getChildNodes();
                for (int j = 0; j < names.getLength(); j++) {
                    Node r = names.item(j);
                    if (r.getNodeType() == Node.ELEMENT_NODE) {
                        Element name = (Element) r;
                        if(name.getTextContent().contains(zz)||name.getTextContent().equals("")) {
                            arr1[i][count] = name.getTextContent();
                        }else{
                            arr1[i][count] = Integer.parseInt(name.getTextContent());
                        }
                        count++;

                    }
                }
            }
        }


        if (pivot == 1) {
            for (int k = 0; k < list.getLength()+1; k++) {

                for (int j = 0; j < nocol; j++) {
                    //System.out.print(arr1[k][j]);
                    //System.out.printf("   ") ;
                    System.out.format("%32s", arr1[k][j]);

                }
                System.out.println();
                System.out.println();

            }
            System.out.printf("                                             ------------------------------------") ;
            System.out.println();
            System.out.println();


            Object[][] hh= new Object[list.getLength()][nocol] ;
            int ee=0;
            for(int i=1;i<list.getLength()+1;i++){
                for(int j=0;j<nocol;j++){
                    hh[ee][j]=arr1[i][j] ;
                }
                ee++ ;
            }

            return hh;
        } else if (pivot == 3) {
            int b = 0;
            ArrayList<Object> yy =new ArrayList<>() ;
            for (int i = 0; i < this.arr.length; i++) {
                for (int j = 0; j < nocol; j++) {
                    if (arr1[0][j].toString().toLowerCase().equals(this.arr[i].toLowerCase())) {
                        b++;
                        yy.add(arr1[0][j]);
                        break;

                    }
                }
            }
            Object[][] n = new Object[list.getLength()+1][b];
            for(int j=0 ;j<b;j++){
                n[0][j]=yy.get(j) ;
            }
            int v = 0;
            for (int t = 0; t < this.arr.length; t++) {
                for (int j = 0; j < nocol; j++) {
                    if (arr1[0][j].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {
                        for (int k = 1; k < list.getLength()+1; k++) {
                            n[k][v] = arr1[k][j];

                        }
                        v++;
                    }
                }
            }
            for (int k = 0; k < list.getLength()+1; k++) {
                for (int j = 0; j < b; j++) {
                    System.out.format("%32s", n[k][j]);

                }
                System.out.println();
                System.out.println();

            }
            System.out.printf("                                             ------------------------------------") ;
            System.out.println();
            System.out.println();

            Object[][] hh= new Object[list.getLength()][b] ;
            int ee=0;
            for(int i=1;i<list.getLength()+1;i++){
                for(int j=0;j<b;j++){
                    hh[ee][j]=n[i][j] ;
                }
                ee++ ;
            }

            return hh;

        } else if(pivot==2) {
            int norows =0 ;
            ArrayList<Object> pp = new ArrayList<>() ;
            for(int i =0;i<list.getLength()+1;i++){
                for(int j=0 ;j<nocol;j++){
                    if(arr1[0][j].toString().toLowerCase().equals(colname.toLowerCase())&&i!=0){
                        switch (ope){
                            case ">":
                                if (arr1[i][j] != "") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) > limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;

                                        }
                                    }
                                    else {
                                        if ((int) arr1[i][j] > limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }
                                }
                                break;
                            case "<":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) < limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] < limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }

                                }
                                break;
                            case "=":
                                if( c==1 && arr1[i][j]!=""){
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) == limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] == limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }
                                }
                                else if (c==2 && arr1[i][j]!=""){

                                    if(arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())){
                                        norows ++ ;
                                        pp.add(arr1[0][j]) ;
                                    }
                                }
                                break;

                            case "<=":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) <= limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] <= limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }

                                }
                                break;
                            case ">=":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) >= limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] >= limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }

                                }
                                break;
                            case "!=":
                                if( c==1&&arr1[i][j]!=""){
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) != limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] != limit) {
                                            norows++;
                                            pp.add(arr1[0][j]) ;
                                        }

                                    }
                                }
                                else if (c==2&&arr1[i][j]!=""){
                                    if(!arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())){
                                        norows ++ ;
                                        pp.add(arr1[0][j]) ;
                                    }
                                }
                                break;
                        }
                    }
                }
            }
            norows++ ;
            Object[][] arr3 = new Object[norows][nocol] ;
            int e=1;

            for (int j = 0; j < nocol; j++) {


                for (int i = 1; i < list.getLength()+1; i++) {
                    if(arr1[0][j].toString().toLowerCase().equals(colname.toLowerCase())&&i!=0){

                        switch (ope) {
                            case ">":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] > limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else {
                                        if (Integer.parseInt((String) arr1[i][j]) > limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }

                                break;
                            case "<":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] < limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else{
                                        if (Integer.parseInt ((String)arr1[i][j])  < limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }
                                break;
                            case "=":
                                if (c == 1 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] == limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else{
                                        if (Integer.parseInt ((String)arr1[i][j])  == limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }
                                else if (c == 2 && arr1[i][j] != "") {
                                    if (arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                        for (int k = 0; k < nocol; k++) {
                                            arr3[e][k] = arr1[i][k];
                                        }
                                        e++;
                                    }

                                }
                                break;
                            case "!=":
                                if (c == 1 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] != limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else{
                                        if (Integer.parseInt ((String)arr1[i][j])  != limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }
                                else if (c == 2 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                        for (int k = 0; k < nocol; k++) {
                                            arr3[e][k] = arr1[i][k];
                                        }
                                        e++;
                                    }

                                }
                                break;
                            case "<=":
                                if (arr1[i][j] != ""){
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] <= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else{
                                        if (Integer.parseInt ((String)arr1[i][j])  <= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }
                                break ;
                            case ">=":
                                if(arr1[i][j]!="") {
                                    if (!arr1[i][j].toString().contains(zz)) {
                                        if ((int) arr1[i][j] >= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    } else{
                                        if (Integer.parseInt ((String)arr1[i][j])  >= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                arr3[e][k] = arr1[i][k];
                                            }
                                            e++;
                                        }
                                    }
                                }
                                break ;


                        }
                    }
                }
            }
            for(int i=0;i<nocol;i++){
                arr3[0][i]=arr1[0][i];
            }
            for (int k = 0; k < norows; k++) {
                for (int j = 0; j < nocol; j++) {
                    System.out.format("%32s", arr3[k][j]);

                }
                System.out.println();
                System.out.println();

            }
            System.out.printf("                                             ------------------------------------") ;
            System.out.println();
            System.out.println();

            Object[][] hh= new Object[norows-1][nocol] ;
            int ee=0;
            for(int i=1;i<norows;i++){
                for(int j=0;j<nocol;j++){
                    hh[ee][j]=arr3[i][j] ;
                }
                ee++ ;
            }


            return hh;


        }else{
            int b = 0;
            for (int i = 0; i < this.arr.length; i++) {
                for (int j = 0; j < nocol; j++) {
                    if (arr1[0][j].toString().toLowerCase().equals(this.arr[i].toLowerCase())) {
                        b++;
                        break;

                    }
                }
            }
            int norows =0 ;
            for(int i =0;i<list.getLength()+1;i++){
                for(int j=0 ;j<nocol;j++){
                    if(arr1[0][j].toString().toLowerCase().equals(colname.toLowerCase())&&i!=0){
                        switch (ope){
                            case ">":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) > limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] > limit) {
                                            norows++;
                                        }

                                    }
                                }
                                break;
                            case "<":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) < limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] < limit) {
                                            norows++;
                                        }

                                    }
                                }
                                break;
                            case "=":
                                if( c==1 && arr1[i][j]!=""){

                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) == limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] == limit) {
                                            norows++;
                                        }

                                    }

                                }
                                else if (c==2 && arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                            norows++;
                                        }
                                    }
                                }
                                break;

                            case "<=":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) <= limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] <= limit) {
                                            norows++;
                                        }


                                    }
                                }
                                break;
                            case ">=":
                                if(arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) >= limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] >= limit) {
                                            norows++;
                                        }

                                    }

                                }
                                break;
                            case "!=":
                                if( c==1&&arr1[i][j]!=""){
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (Integer.parseInt((String) arr1[i][j]) != limit) {
                                            norows++;
                                        }
                                    }else {
                                        if ((int) arr1[i][j] != limit) {
                                            norows++;
                                        }

                                    }
                                }
                                else if (c==2&&arr1[i][j]!="") {
                                    if (arr1[i][j].toString().contains(zz)) {
                                        if (!arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                            norows++;
                                        }
                                    }
                                }
                                break;
                        }
                    }
                }
            }
            norows++ ;

            Object[][] arr3 = new Object[norows][b];
            int v = 0;
            int e=1;


            for (int j = 0; j < nocol; j++) {



                for (int i = 1; i < list.getLength()+1; i++) {
                    v=0;
                    if (arr1[0][j].toString().toLowerCase().equals(colname.toLowerCase()) && i != 0) {

                        switch (ope) {
                            case ">":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] > limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  > limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }

                                break;
                            case "<":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] < limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  < limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }
                                break;
                            case "=":
                                if (c == 1 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] == limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  == limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }
                                else if (c == 2 && arr1[i][j] != "") {
                                    if (arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                        for(int k=0;k<nocol;k++) {
                                            for(int t=0;t<this.arr.length;t++){

                                                if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                    arr3[e][v] = arr1[i][k];
                                                    if((b-v)!=1) {
                                                        v++;
                                                    }
                                                    break ;

                                                }
                                            }

                                        }

                                    }
                                }
                                if((norows-e)!=1) {
                                    e++;
                                }
                                break;
                            case "!=":
                                if (c == 1 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] != limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  != limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }
                                else if (c == 2 && arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().toLowerCase().equals(f.toLowerCase())) {
                                        for(int k=0;k<nocol;k++) {
                                            for(int t=0;t<this.arr.length;t++){

                                                if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                    arr3[e][v] = arr1[i][k];
                                                    if((b-v)!=1) {
                                                        v++;
                                                    }
                                                    break;

                                                }
                                            }

                                        }

                                    }
                                }
                                if((norows-e)!=1) {
                                    e++;
                                }

                                break;
                            case "<=":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] <= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  <= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }
                                break;
                            case ">=":
                                if (arr1[i][j] != "") {
                                    if (!arr1[i][j].toString().contains(zz)) {

                                        if ((int) arr1[i][j] >= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    } else{
                                        if (Integer.parseInt((String)arr1[i][j])  >= limit) {
                                            for (int k = 0; k < nocol; k++) {
                                                for (int t = 0; t < this.arr.length; t++) {

                                                    if (arr1[0][k].toString().toLowerCase().equals(this.arr[t].toLowerCase())) {

                                                        arr3[e][v] = arr1[i][k];
                                                        if ((b - v) != 1) {
                                                            v++;
                                                        }
                                                        break;

                                                    }
                                                }

                                            }
                                        }

                                    }
                                }

                                if((norows-e)!=1) {
                                    e++;
                                }
                                break;


                        }
                    }
                }


            }
            v=0;
            for (int i = 0; i < this.arr.length; i++) {
                for (int j = 0; j < nocol; j++) {
                    if (arr1[0][j].toString().toLowerCase().equals(this.arr[i].toLowerCase())) {
                        arr3[0][v]=arr1[0][j] ;

                    }
                }
                v++ ;
            }
            for (int k = 0; k < norows; k++) {
                for (int j = 0; j < b; j++) {
                    System.out.format("%32s", arr3[k][j]);

                }
                System.out.println();
                System.out.println();

            }
            System.out.printf("                                             ------------------------------------") ;
            System.out.println();
            System.out.println();


            Object[][] hh= new Object[norows-1][b] ;
            int ee=0;
            for(int i=1;i<norows;i++){
                for(int j=0;j<b;j++){
                    hh[ee][j]=arr3[i][j] ;
                }
                ee++ ;
            }

            return hh;
        }
    }
    @Override
    public int executeUpdateQuery(String query) throws SQLException {
        query =query.toLowerCase();
        if(query.matches("(?i)^([ ]*insert[ ]+into[ ]+[a-z_0-9]+[ ]*([(].*[)])*[ ]*values[ ]*[(].*[)][ ]*)$")){
            String temp = query.substring(query.toLowerCase().indexOf("into"),query.toLowerCase().indexOf("("));
            String[] arrTemp = temp.split("[ ]+");
            table = new Table(databaseName ,arrTemp[1],query.substring(query.toLowerCase().indexOf("("),query.toLowerCase().indexOf(")")));
            try {
                return table.insertInto(query.substring(query.toLowerCase().indexOf("("),query.toLowerCase().indexOf(")")),query.substring(query.toLowerCase().lastIndexOf("("),query.toLowerCase().lastIndexOf(")")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }else if(query.matches("(?i)^([ ]*delete[ ]+from[ ]+[a-z_0-9]+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9\']+[ ]*))?[ ]*)$")){
            String temp = query.substring(query.toLowerCase().indexOf("from"));
            String[] arrTemp = temp.split("[ ]+");
            table = new Table(databaseName ,arrTemp[1],arrTemp[0]);
            if(query.contains("where")) {
                temp = query.substring(query.toLowerCase().indexOf("where"));
                arrTemp = temp.split("[ =]+");
                return table.deleteFromTable(arrTemp[1], arrTemp[2]);
            }else{
                return table.deleteFromTable(null, null);
            }


        }else if(query.matches("(?i)^([ ]*update[ ]+[a-z_0-9]+[ ]+" +
                "set[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*,[ ]*)*([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+)+" +
                "([ ]+where[ ]+([a-z_0-9]+[ ]*=[ ]*[a-z_0-9']+[ ]*))?[ ]*)$")) {
            String temp = query.substring(query.toLowerCase().indexOf("update"));
            String[] arrTemp = temp.split("[ ]+");
            table = new Table(databaseName ,arrTemp[1],arrTemp[0]);
            return table.update(query.toLowerCase().substring(query.toLowerCase().indexOf("set")));

        }else{
            throw new SQLException();
        }
        return 0;
    }
    public boolean dropIfExists(String path) throws SQLException {
        File file = new File(path);
        return file.exists();
    }

}
