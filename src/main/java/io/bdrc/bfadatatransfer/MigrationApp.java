package io.bdrc.bfadatatransfer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RiotException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.bdrc.ewtsconverter.EwtsConverter;


public class MigrationApp 
{

    public static class PropInfo
    {
        String mappedProp;
        boolean inArray;
        boolean isObjectProp;

        public PropInfo(String mappedProp, boolean inArray, boolean isObjectProp)
        {
            this.mappedProp = mappedProp;
            this.inArray = inArray;
            this.isObjectProp = isObjectProp;
        }
    }

    public static final String DESCRIPTION_PREFIX = "http://onto.bdrc.io/ontology/description#";
    public static final String ROOT_PREFIX = "http://purl.bdrc.io/ontology/root#";
    public static final String LINEAGE_PREFIX = "http://purl.bdrc.io/ontology/lineage#";
    public static final String OFFICE_PREFIX = "http://purl.bdrc.io/ontology/office#";
    public static final String OUTLINE_PREFIX = "http://purl.bdrc.io/ontology/outline#";
    public static final String PERSON_PREFIX = "http://purl.bdrc.io/ontology/person#";
    public static final String PLACE_PREFIX = "http://purl.bdrc.io/ontology/place#";
    public static final String TOPIC_PREFIX = "http://purl.bdrc.io/ontology/topic#";
    public static final String VOLUMES_PREFIX = "http://purl.bdrc.io/ontology/volumes#";
    public static final String WORK_PREFIX = "http://purl.bdrc.io/ontology/work#";
    public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
    
    public static final int INDEX_LIMIT_SIZE = 30000;
    
    public static final EwtsConverter converter = new EwtsConverter();

    // extract tbrc/ folder of exist-db backup here:
    public static final String DATA_DIR = "../xmltoldmigration/tbrc-jsonld/";
    public static final String OUTPUT_DIR = "tbrc-bfa/";

    public static Map<String, List<String>> textIndex = new TreeMap<String, List<String>>();
    public static final Map<String, List<String>> authorTextMap = new HashMap<String, List<String>>();
    
    public static final Map<String, String> outlineWorkTitleMap = new HashMap<String, String>();
    public static final Map<String, String> workOutlineIdMap = new HashMap<String, String>();
    
    public static final Map<String, ArrayNode> workVolumesMap = new HashMap<String, ArrayNode>();
    
    public static List<String> nodeList = new ArrayList<String>();

    public static final ObjectMapper om = new ObjectMapper();

    public static final Map<String, PropInfo> propMapping = new HashMap<String,PropInfo>();

    static {
        propMapping.put("status", new PropInfo("status", false, false));
        propMapping.put("archiveInfo_status", new PropInfo("archiveInfo_status", false, false));
        propMapping.put("archiveInfo_vols", new PropInfo("archiveInfo_vols", false, false));
        propMapping.put("bibliographicalTitle", new PropInfo("title", true, false));
        propMapping.put("captionTitle", new PropInfo("title", true, false));
        propMapping.put("colophonTitle", new PropInfo("title", true, false));
        propMapping.put("copyrightPageTitle", new PropInfo("title", true, false));
        propMapping.put("coverTitle", new PropInfo("title", true, false));
        propMapping.put("dkarChagTitle", new PropInfo("title", true, false));
        propMapping.put("fullTitle", new PropInfo("title", true, false));
        propMapping.put("halfTitle", new PropInfo("title", true, false));
        propMapping.put("incipit", new PropInfo("title", true, false));
        propMapping.put("otherTitle", new PropInfo("title", true, false));
        propMapping.put("runningTitle", new PropInfo("title", true, false));
        propMapping.put("sectionTitle", new PropInfo("title", true, false));
        propMapping.put("spineTitle", new PropInfo("title", true, false));
        propMapping.put("titlePageTitle", new PropInfo("title", true, false));
        propMapping.put("subtitle", new PropInfo("title", true, false));
        propMapping.put("pubinfo_printType", new PropInfo("printType", false, false));
        propMapping.put("pubinfo_publisherDate", new PropInfo("publisherDate", false, false));
        propMapping.put("pubinfo_publisherLocation", new PropInfo("publisherLocation", false, false));
        propMapping.put("outlinedBy", new PropInfo("outlinedBy", false, true));
        propMapping.put("isOutlineOf", new PropInfo("isOutlineOf", false, true));
        propMapping.put("hasCreator", new PropInfo("hasCreator", true, true));
        propMapping.put("hasArtist", new PropInfo("hasArtist", true, true));
        propMapping.put("hasAttributedAuthor", new PropInfo("hasCreator", true, true));
        propMapping.put("hasBard", new PropInfo("hasCreator", true, true));
        propMapping.put("hasCalligrapher", new PropInfo("hasCreator", true, true));
        propMapping.put("hasCommentator", new PropInfo("hasCreator", true, true)); // ?
        propMapping.put("hasContributingAuthor", new PropInfo("hasCreator", true, true));
        propMapping.put("hasEditor", new PropInfo("hasCreator", true, true)); // ?
        propMapping.put("hasMainAuthor", new PropInfo("hasCreator", true, true)); 
        propMapping.put("hasPandita", new PropInfo("hasCreator", true, true));
        propMapping.put("hasScribe", new PropInfo("hasCreator", true, true));
        propMapping.put("hasTerton", new PropInfo("hasCreator", true, true));
        propMapping.put("hasTranslator", new PropInfo("hasCreator", true, true)); // ?
        propMapping.put("name", new PropInfo("name", true, false));
        propMapping.put("bodhisattvaVowName", new PropInfo("name", true, false));
        propMapping.put("commonName", new PropInfo("name", true, false));
        propMapping.put("corporateName", new PropInfo("name", true, false));
        propMapping.put("familyName", new PropInfo("name", true, false));
        propMapping.put("finalOrdinationName", new PropInfo("name", true, false));
        propMapping.put("fiestOrdinationName", new PropInfo("name", true, false));
        propMapping.put("gTerStonTitle", new PropInfo("name", true, false));
        propMapping.put("officeTitle", new PropInfo("name", true, false));
        propMapping.put("otherName", new PropInfo("name", true, false));
        propMapping.put("penName", new PropInfo("name", true, false));
        propMapping.put("pesonalName", new PropInfo("name", true, false));
        propMapping.put("personTitle", new PropInfo("name", true, false));
        propMapping.put("primaryName", new PropInfo("name", true, false));
        propMapping.put("primaryTitle", new PropInfo("name", true, false));
        propMapping.put("tulkuTitle", new PropInfo("name", true, false));
        propMapping.put("secretInitiatoryName", new PropInfo("name", true, false));
    }

    public static void createDirIfNotExists(String dir) {
        File theDir = new File(dir);
        if (!theDir.exists()) {
            System.out.println("creating directory: " + dir);
            try{
                theDir.mkdir();
            } 
            catch(SecurityException se){
                System.err.println("could not create directory, please fasten your seat belt");
            }        
        }
    }

    public static String removePrefix(String uri) {
        int i = uri.indexOf('#');
        if (i < 0) {
            i = uri.lastIndexOf('/');
            return uri.substring(i+1);
        }
        return uri.substring(i+1);
    }

    public static void writeToIndex(String titleOrName, String id, String type) {
        if (titleOrName == null || titleOrName.isEmpty()) return;
        List<String> ridList = textIndex.get(titleOrName);
        if (ridList == null) {
            ridList = new ArrayList<String>();
            textIndex.put(titleOrName, ridList);
            ridList.add(id);
        } else {
            if (!ridList.contains(id)) {
                ridList.add(id);
            }
        }
    }

    public static Model modelFromFile(File f) {
        InputStream fStream;
        try {
            fStream = new FileInputStream(f);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return null;
        }
        Model model = ModelFactory.createDefaultModel();
        try {
            model.read(fStream, null, "JSON-LD") ;
        } catch (RiotException e) {
            System.err.println(f.getAbsolutePath());
            e.printStackTrace();
            return null;
        }
        return model;
    }

    public static String getFullUrlFromBaseFileName(String baseName, String type) {
        switch(type) {
        case "work":
            return WORK_PREFIX+baseName;
        case "volume":
            return VOLUMES_PREFIX+baseName;
        case "person":
            return PERSON_PREFIX+baseName;
        default:
            return OUTLINE_PREFIX+baseName;
        }
    }

    public static void addAuthorMapping(String workId, String authorId) {
        List<String> workList = authorTextMap.get(authorId);
        if (workList == null) {
            workList = new ArrayList<String>();
            authorTextMap.put(authorId, workList);
        }
        workList.add(workId);
    }

    public static void addToOutput(ObjectNode output, PropInfo pInfo, String value) {
        if (pInfo.inArray) {
            ArrayNode a = (ArrayNode) output.get(pInfo.mappedProp);
            if (a == null) {
                a = om.createArrayNode();
                output.set(pInfo.mappedProp, a);
            }
            a.add(value);
        } else {
            output.put(pInfo.mappedProp, value);
        }
    }

    public static void fillTreeProperties(Model m, Resource r, ObjectNode rootNode, String type, String baseName, String rootBaseName) {
        if (type.equals("person")) {
            String queryString = "PREFIX per: <"+PERSON_PREFIX+">\n"
                    + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                    + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                    + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
                    + "SELECT ?eventType ?circa\n"
                    + "WHERE {\n"
                    + "  {\n"
                    + "     ?b rdf:type per:Death .\n"
                    + "     ?b per:event_circa ?circa .\n"
                    + "     BIND (\"death\" AS ?eventType)\n"
                    + "  } UNION {\n"
                    + "     ?b rdf:type per:Birth .\n"
                    + "     ?b per:event_circa ?circa .\n"
                    + "     BIND (\"birth\" AS ?eventType)\n"
                    + "  }\n"
                    + "}\n" ;
            Query query = QueryFactory.create(queryString) ;
            try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
              ResultSet results = qexec.execSelect() ;
              while (results.hasNext()) {
                QuerySolution soln = results.nextSolution() ;
                String circaString = soln.get("circa").asLiteral().getString();
                String eventType = soln.get("eventType").asLiteral().getString();
                rootNode.put(eventType, circaString);
              }
            }
//        } else if (type.equals("work")) {
//            Property p = m.getProperty(OUTLINE_PREFIX+"hasNode");
        } else if (type.equals("outline")) {
            Property p = m.getProperty(OUTLINE_PREFIX+"hasNode");
            StmtIterator propIter = r.listProperties(p);
            while(propIter.hasNext()) {
                Statement s = propIter.nextStatement();
                Resource o = s.getResource();
                String oid = removePrefix(o.getURI());
                ArrayNode a = (ArrayNode) rootNode.get("nodes");
                if (a == null) {
                    a = om.createArrayNode();
                    rootNode.set("nodes", a);
                }
                ObjectNode nodeNode = om.createObjectNode();
                nodeNode.put("id", oid);
                if (nodeList.contains(oid)) {
                    System.err.println("outline node loop detected: node "+oid+" already encountered (treating "+rootBaseName+")");
                    return;
                }
                nodeList.add(oid);
                fillResourceInNode(m, o, oid, nodeNode, rootNode, rootBaseName, type);
                if (nodeNode.size() > 1) {
                    a.add(nodeNode);                    
                }
            }
        }
    }
    
    public static String getLabel(Model m, Resource r) {
        String res = null;
        Property p = m.getProperty(RDFS_PREFIX+"label");
        StmtIterator propIter = r.listProperties(p);
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            Literal l = s.getLiteral();
            String lang = l.getLanguage();
            if (lang.equals("bo-x-ewts")) {
                return converter.toUnicode(l.getString());
            }
        }
        return res;
    }
    
    public static void fillResourceInNode(Model m, Resource r, String rName, ObjectNode currentNode, ObjectNode rootNode, String rootName, String type) {
        String label = null;
        if (type.equals("person") || type.equals("work")) {
            label = getLabel(m, r);
            writeToIndex(label, rootName, type);
            addToOutput(currentNode, new PropInfo((type == "person" ? "name" : "title"), true, false), label);
            if (type.equals("work")) {
                String outlineId = workOutlineIdMap.get(rootName);
                if (outlineId != null) {
                    outlineWorkTitleMap.put(outlineId, label);
                }
            }
        }
        StmtIterator propIter = r.listProperties();
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            Property p = s.getPredicate();
            String pBaseName = removePrefix(p.getURI());
            //System.out.println(pBaseName);
            PropInfo pInfo = propMapping.get(pBaseName);
            if (pInfo == null) continue;
            if (pInfo.isObjectProp) {
                Resource o = s.getResource();
                String oid = removePrefix(o.getURI());
                addToOutput(currentNode, pInfo, oid);
                if (pInfo.mappedProp.equals("hasCreator")) {
                    addAuthorMapping(rName, oid);
                }
                if (pInfo.mappedProp.equals("isOutlineOf")) {
                    workOutlineIdMap.put(oid, rootName);
                }
            } else {
                if (type.equals("outline") && rootName.equals(rName) /* && (pInfo.mappedProp.equals("name") || pInfo.mappedProp.equals("title"))*/) {
                    // not interested in outline's main title, should be the same as the work
                    continue;
                }
                if (type.equals("outline") && pInfo.mappedProp.equals("name")) {
                    // not interested in outline names, just titles
                    continue;
                }
                if (!type.equals("work") && pInfo.mappedProp.equals("status")) {
                    // only interested in work status
                    continue;
                }
                Literal l = s.getLiteral();
                // TODO: handle non-strings?
                String lang = l.getLanguage();
                if (lang.isEmpty()) {
                    addToOutput(currentNode, pInfo, l.getString());
                } else if (lang.equals("bo-x-ewts")) {
                    String uniString = converter.toUnicode(l.getString());
                    if (label != null && label.equals(uniString)) {
                        continue;
                    }
                    writeToIndex(uniString, rootName+'-'+rName, type);
                    addToOutput(currentNode, pInfo, uniString);
                    if (type == "outline") break; // just one title per outline
                }
            }
        }
        fillTreeProperties(m, r, rootNode, type, rName, rootName);
    }
    
    private static void fillVolumes(Model m, Resource r, String mainResourceName) {
        String volumesId = removePrefix(r.getURI());
        String workId = 'W'+volumesId.substring(1);
        Property p = m.getProperty(VOLUMES_PREFIX+"hasVolume");
        StmtIterator propIter = r.listProperties(p);
        ArrayNode volumesNode = om.createArrayNode();
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            Resource o = s.getResource();
            String oid = removePrefix(o.getURI());
            String imageGroupId = oid.substring(oid.lastIndexOf("_") + 1);
            ObjectNode volumeNode = om.createObjectNode();
            volumeNode.put("id", imageGroupId);
            int volnum = o.getProperty(m.getProperty(VOLUMES_PREFIX+"number")).getInt();
            Statement totalPag = o.getProperty(m.getProperty(VOLUMES_PREFIX+"pages_total"));
            if (totalPag != null) {
                int totalPagInt = totalPag.getInt();
                volumeNode.put("total", totalPagInt);
                volumeNode.put("num", volnum);
                volumesNode.add(volumeNode);
            }
        }
        if (volumesNode.size() > 0) {
            workVolumesMap.put(workId, volumesNode);
        }
    }
    
    public static void migrateOneFile(File file, String type) {
        if (file.isDirectory()) return;
        String fileName = file.getName();
        if (!fileName.endsWith(".jsonld")) return;
        String baseName = fileName.substring(0, fileName.length()-7);
        ObjectNode output = om.createObjectNode();
        String outfileName = baseName+".json";
        outfileName = OUTPUT_DIR+type+"s/"+outfileName;
        Model m = modelFromFile(file);
        if (m == null) {
            System.err.println("cannot read model from file "+file.getAbsolutePath());
            return;
        }
        String mainResourceName = getFullUrlFromBaseFileName(baseName, type);
        Resource mainR = m.getResource(mainResourceName);
        if (mainR == null) {
            System.err.println("unable to find resource "+mainResourceName);
            return;
        }
        if (type.equals("volume")) {
            fillVolumes(m, mainR, mainResourceName);
            return;
        }
        if (type.equals("person")) {
            List<String> workList = authorTextMap.get(baseName);
            if (workList == null) return; // if a person didn't write books, we just skip
            ArrayNode a = om.valueToTree(workList);
            output.set("creatorOf", a);
        }
        if (type.equals("work")) {
            ArrayNode volumesNode = workVolumesMap.get(baseName);
            if (volumesNode != null) {
                output.set("volumeMap", volumesNode);
            }
        }
        if (type.equals("outline")) {
            nodeList = new ArrayList<String>();
        }
        fillResourceInNode(m, mainR, baseName, output, output, baseName, type);
        try {
            om.writeValue(new File(outfileName), output);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // System.exit(0);
    }

    public static void dumpMapInFile(Map<String,List<String>> textIndexChunk, String fileName) {
        System.out.println("dumping "+textIndexChunk.size()+" entries to "+ fileName);
        try {
            om.writeValue(new File(OUTPUT_DIR+fileName), textIndexChunk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void dumpIndex(String type) {
        int i = 0;
        int total = 0;
        Map<String,List<String>> currentChunk = new HashMap<String,List<String>>();
        for (Entry<String, List<String>> entry : textIndex.entrySet()) {
            total = total + 1;
            currentChunk.put(entry.getKey(), entry.getValue());
            if (total > INDEX_LIMIT_SIZE) {
                dumpMapInFile(currentChunk, type+"Index-"+i+".json");
                i = i + 1;
                total = 0;
                currentChunk = new HashMap<String,List<String>>();
            }
        }
        if (!type.equals("volume")) {
            dumpMapInFile(currentChunk, type+"Index-"+i+".json");
        }
        textIndex = new HashMap<String, List<String>>();
    }
    
    public static void migrateType(String type) {
        textIndex = new HashMap<String, List<String>>();
        createDirIfNotExists(OUTPUT_DIR+type+"s");
        String dirName = DATA_DIR+type+"s";
        File[] files = new File(dirName).listFiles();
        System.out.println("converting "+files.length+" "+type+" files");
        //Stream.of(files).parallel().forEach(file -> migrateOneFile(file, type));
        Stream.of(files).forEach(file -> migrateOneFile(file, type));
        dumpIndex(type);
    }

    public static void main( String[] args )
    {
        createDirIfNotExists(OUTPUT_DIR);
        long startTime = System.currentTimeMillis();
        //migrateType("outline");
        migrateType("volume");
        migrateType("work");
        //migrateType("person");
        System.out.println("dumping "+outlineWorkTitleMap.size()+" entries to outlineWorkTitle.json");
        try {
            om.writeValue(new File(OUTPUT_DIR+"outlineWorkTitle.json"), outlineWorkTitleMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //migrateOneFile(new File("src/test/resources/W12827.jsonld"), "work");
        //migrateOneFile(new File("src/test/resources/P1583.jsonld"), "person");
        //migrateOneFile(new File("src/test/resources/O2DB87572.jsonld"), "outline");
        //  migrateOneFile(new File("O1LS2931.jsonld"), "outline");
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("done in "+estimatedTime+" ms");
    }
}
