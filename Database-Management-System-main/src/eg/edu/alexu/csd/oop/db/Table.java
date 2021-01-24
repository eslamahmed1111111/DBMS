package eg.edu.alexu.csd.oop.db;
//import com.sun.java.util.jar.pack.Package;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
public class Table {
    private String name;
    private String databaseName;
    private String columns;
    private String p,o;

    public Table(String databaseName, String name, String columns) {
        this.name = name;
        this.databaseName = databaseName;
        this.columns = columns;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public int insertInto(String columnsNames, String values) throws FileNotFoundException, SQLException {
        String path , XSDPath;
        if(databaseName!= null) {
            path = databaseName + "\\" + name + ".xml";
            XSDPath = databaseName + "\\" + name + ".xsd";
        }else{
            path = name + ".xml";
            XSDPath = name + ".xsd";
        }
        File file = new File(path);
        if(!file.exists()) {
            createTable(false);
        }
            File XSDFile = new File(XSDPath);
            String[] columnsNamesArray = columnsNames.split("[( ,)]+");
            String[] valuesArray = values.split("[( ,)]+");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilderFactory XSDFactory = DocumentBuilderFactory.newInstance();
            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(file);
                Document XSDDoc = XSDFactory.newDocumentBuilder().parse(XSDFile);
                Element element = document.createElement("row");
//                  creating columns
                NodeList list = XSDDoc.getElementsByTagName("xs:element");
                Node node;
                for(int i = 1 ; i < list.getLength() ; i++){
                    node = list.item(i);
                    boolean found = columnsNames.equals(values);
                    String temp=node.getAttributes().item(0).toString();
                    temp = temp.substring(temp.indexOf("\"")+1,temp.lastIndexOf("\""));
                    for(int j = 1 ; j < columnsNamesArray.length ; j++){
                        if(temp.equals(columnsNamesArray[j])||found){
                            Text value;
                            Element subElement = document.createElement(temp);
                            if(found){
                                value = document.createTextNode(valuesArray[i]);
                            }else{
                                value = document.createTextNode(valuesArray[j]);
                            }
                            subElement.appendChild(value);
                            element.appendChild(subElement);
                            found=true;
                            break;
                        }
                    }
                    if(!found) {
                        Element subElement = document.createElement(temp);
                        Text value = document.createTextNode(null);
                        subElement.appendChild(value);
                        element.appendChild(subElement);
                    }
                }
                Element root = document.getDocumentElement();
                root.appendChild(element);
                DOMSource source = new DOMSource(document);
                Result result = new StreamResult(file.getPath());
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.transform(source, result);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 1;
    }
    public int deleteFromTable(String columnName, String testValue) throws SQLException {
        int count=0;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            File file = new File(databaseName + "\\" + name + ".xml");
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(file.getPath());
            if(columnName==null&&testValue==null){
                //delete all elements
                NodeList all = document.getElementsByTagName("row");
                while (all.getLength()!=0){
                    Node node = all.item(0);
                    document.getDocumentElement().removeChild(node);
                    count++;
                }
            }else {

                NodeList list = document.getElementsByTagName(columnName);
                while (list.getLength()!=0) {
                    boolean finished = false;
                    for(int i = 0 ; i < list.getLength();i++) {
                        String content = list.item(i).getTextContent();
                        if (content.contentEquals(testValue)) {
                            Node p = list.item(i).getParentNode();
                            document.getDocumentElement().removeChild(p);
                            count++;
                            break;
                        }
                        if(i==list.getLength()-1){
                            finished = true;
                        }
                    }
                    if(finished){
                        break;
                    }
                }
            }
            DOMSource source = new DOMSource(document);
            Result result = new StreamResult(file.getPath());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(source, result);
        }
        catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        catch (TransformerConfigurationException e) {
            e.printStackTrace();
        }
        catch (TransformerException e) {
            e.printStackTrace();
        }
        catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
    public boolean createTable(boolean dropIfExists) throws SQLException,FileNotFoundException {
        if(databaseName == null){
            throw new SQLException();
        }
        String[] column = columns.split("[( ,)]+");
        if (dropIfExists) {
            File file = new File(databaseName + "\\" + name + ".xml");
            file.delete();
        }
//        writing xml file using DOM parser
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.newDocument();
            Element root = document.createElement(name);

            document.appendChild(root);

//            writing from document(temp) to file
            DOMSource source = new DOMSource(document);
            File file = new File(databaseName + "\\" + name + ".xml");
            Result result = new StreamResult(file);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(source, result);

            /*
            * XSD FILE
            * */
            document = builder.newDocument();
            Element xsSchema = document.createElement("xs:schema");
            xsSchema.setAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema");
            Element xsElement = document.createElement("xs:element");
            xsElement.setAttribute("name", "row");
            Element xsComplexType = document.createElement("xs:complexType");
            Element xsSequence = document.createElement("xs:sequence");
            for (int i = 1; i < column.length; i++) {
                Element xsCell = document.createElement("xs:element");
                xsCell.setAttribute("name", column[i]);
                i++;
                xsCell.setAttribute("type", column[i]);
                xsSequence.appendChild(xsCell);
            }
            xsComplexType.appendChild(xsSequence);
            xsElement.appendChild(xsComplexType);
            xsSchema.appendChild(xsElement);
            document.appendChild(xsSchema);
            source = new DOMSource(document);
            file = new File(databaseName + "\\" + name + ".xsd");
            result = new StreamResult(file);
            transformerFactory = TransformerFactory.newInstance();
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(source, result);

//            SchemaFactory schemaFactory =
//                    SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
//            Schema schema = schemaFactory.newSchema(new File(databaseName+"\\"+name+".xsd"));
//            Validator validator = schema.newValidator();
//            validator.validate(new StreamSource(new File(databaseName+"\\"+name+".xml")));

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
//        } catch (SAXException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
        }
        return !dropIfExists;
    }
    public void dropTable(boolean dropIfExists) {
        if (dropIfExists) {
            File file = new File(databaseName + "\\" + name + ".xml");
            File fileXSD = new File(databaseName + "\\" + name + ".xsd");
            file.delete();
            fileXSD.delete();
        }
    }
    public boolean dropIfExists(String path) throws SQLException {
        File file = new File(path);
        return file.exists();
    }
    public int update(String query) throws SQLException {
        int count = 0;
        try {
            File file = new File(databaseName + "\\" + name + ".xml");
            if(!file.exists()){
                throw  new SQLException();
            }
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder;
            docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(databaseName + "\\" + name + ".xml");
            NodeList nodeList = doc.getElementsByTagName("row");
            boolean trueCondition = false;
            boolean changed = false;
            String setTemp;
            String conditionTemp ;
            String[] set ;
            String[] condition;
            if(query.contains("where")) {
                setTemp = query.substring(query.indexOf("set"),query.indexOf("where"));
                set = setTemp.split("[ =,]+");
                conditionTemp = query.substring(query.indexOf("where"));
                condition = conditionTemp.split("[ =]+");
                for (int i = 0; i < nodeList.getLength(); i++) {
                    changed = false;
                    trueCondition = false;
                    NodeList nodes = nodeList.item(i).getChildNodes();
                    for (int j = 0; j < nodes.getLength(); j++) {
                        String value = nodes.item(j).getTextContent();
                        String nodeName = nodes.item(j).getNodeName();
                        if (nodeName.equals(condition[1]) && value.equals(condition[2])) {
                            trueCondition = true;
                            break;
                        }
                    }
                    if (trueCondition) {
                        for (int k = 1; k < set.length; k += 2) {
                            NodeList nodesChange = nodeList.item(i).getChildNodes();
                            for (int j = 0; j < nodesChange.getLength(); j++) {
                                if (nodesChange.item(j).getNodeName().equals(set[k])) {
                                    if ((set[k + 1].contains("'") && nodesChange.item(j).getTextContent().contains("'")) ||
                                            (!set[k + 1].contains("'") && !nodesChange.item(j).getTextContent().contains("'"))) {
                                        nodesChange.item(j).setTextContent(set[k + 1]);
                                        changed = true;

                                    }else{
                                        throw new IOException();
                                    }
                                }
                            }
                        }
                    }
                    if(changed){
                        count++;
                    }
                }
            }else{
                setTemp = query.substring(query.indexOf("set"));
                set = setTemp.split("[ =,]+");
                for (int i = 0; i < nodeList.getLength(); i++) {
                    changed=false;
                        for (int k = 1; k < set.length; k += 2) {
                            NodeList nodesChange = nodeList.item(i).getChildNodes();
                            for (int j = 0; j < nodesChange.getLength(); j++) {
                                if (nodesChange.item(j).getNodeName().equals(set[k])) {
                                    if ((set[k + 1].contains("'") && nodesChange.item(j).getTextContent().contains("'")) ||
                                            (!set[k + 1].contains("'") && !nodesChange.item(j).getTextContent().contains("'"))) {
                                        nodesChange.item(j).setTextContent(set[k + 1]);
                                        changed = true;

                                    }else {
//                                        throw new IOException();
                                    }
                                }

                            }
                        }
                        if(changed){
                            count++;
                        }
                    }

                }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(databaseName + "\\" + name + ".xml"));
            transformer.transform(source, result);
            return count;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return count;
    }
}