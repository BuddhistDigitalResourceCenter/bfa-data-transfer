package io.bdrc.bfadatatransfer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

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
        boolean isSimpleObjectProp;
        boolean toIndex;

        public PropInfo(String mappedProp, boolean inArray, boolean isObjectProp, boolean isSimpleObjectProp, boolean toIndex)
        {
            this.mappedProp = mappedProp;
            this.inArray = inArray;
            this.isObjectProp = isObjectProp;
            this.isSimpleObjectProp = isSimpleObjectProp;
            this.toIndex = toIndex;
        }
    }

    public static final String ONTOLOGY_PREFIX = "http://purl.bdrc.io/ontology/core/";
    public static final String ADMIN_PREFIX = "http://purl.bdrc.io/ontology/admin/";
    public static final String DATA_PREFIX = "http://purl.bdrc.io/data/";
    public static final String RESOURCE_PREFIX = "http://purl.bdrc.io/resource/";
    public static final String SKOS_PREFIX = "http://www.w3.org/2004/02/skos/core#";
    public static final String VCARD_PREFIX = "http://www.w3.org/2006/vcard/ns#";
    public static final String TBR_PREFIX = "http://purl.bdrc.io/ontology/toberemoved/";
    public static final String OWL_PREFIX = "http://www.w3.org/2002/07/owl#";
    public static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#";
    public static final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";
    
    public static final int RESTRICTED_CHINA = 0;
    public static final int RESTRICTED_TBRC = 1;
    public static final int RESTRICTED_QUALITY = 2;
    public static final int RESTRICTED_SEALED = 3;
    public static final int RESTRICTED_OPEN = 4;
    public static final int RESTRICTED_FAIRUSE = 5;
    
    public static final String BDO = ONTOLOGY_PREFIX;
    public static final String BDR = RESOURCE_PREFIX;
    public static final String ADM = ADMIN_PREFIX;
    
    public static final int INDEX_LIMIT_SIZE = 10000;
    
    public static final EwtsConverter converter = new EwtsConverter();

    // extract tbrc/ folder of exist-db backup here:
    public static final String DATA_DIR = "../xmltoldmigration/tbrc-ttl/";
    public static final String OUTPUT_DIR = "tbrc-bfa/";

    public static Map<String, List<String>> textIndex = new TreeMap<String, List<String>>();
    public static Map<String, List<String>> outlineIndex = new TreeMap<String, List<String>>();
    public static final Map<String, List<String>> authorTextMap = new HashMap<String, List<String>>();
    
    public static final Map<String, String> outlineWorkTitleMap = new HashMap<String, String>();
    public static final Map<String, String> workOutlineIdMap = new HashMap<String, String>();
    
    public static final Map<String, ArrayNode> workVolumesMap = new HashMap<String, ArrayNode>();
    
    public static List<String> nodeList = new ArrayList<String>();

    public static final ObjectMapper om = new ObjectMapper();

    public static final Map<String, PropInfo> propMapping = new HashMap<String,PropInfo>();
    
    public static final Map<String, Integer> newRestrictions = new HashMap<>(); 

    static {
        init();
    }

    public static void init() {
        propMapping.put("workIncipit", new PropInfo("title", true, false, false, false));
        //propMapping.put("pubinfo_printType", new PropInfo("printType", false, false, false));
        propMapping.put("workPublisherDate", new PropInfo("publisherDate", false, false, false, false));
        propMapping.put("workPublisherName", new PropInfo("publisherName", false, false, false, false));
        propMapping.put("workPublisherLocation", new PropInfo("publisherLocation", false, false, false, false));
        propMapping.put("workCreator", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorArtist", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorAttributedAuthor", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorBard", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorCalligrapher", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorCommentator", new PropInfo("hasCreator", true, true, false, false)); // ?
        propMapping.put("creatorContributingAuthor", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorEditor", new PropInfo("hasCreator", true, true, false, false)); // ?
        propMapping.put("creatorMainAuthor", new PropInfo("hasCreator", true, true, false, false)); 
        propMapping.put("creatorPandita", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorScribe", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorTerton", new PropInfo("hasCreator", true, true, false, false));
        propMapping.put("creatorTranslator", new PropInfo("hasCreator", true, true, false, false)); // ?
        propMapping.put("workTitle", new PropInfo("title", true, true, true, true));
        propMapping.put("personName", new PropInfo("name", true, true, true, true));
        File newRestrictionsFile = new File("allworks.csv");
        FileInputStream fis;
        try {
            fis = new FileInputStream(newRestrictionsFile);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return;
        }
        
        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
     
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                switch(parts[1]) {
                case "FairUse":
                    newRestrictions.put(parts[0], RESTRICTED_FAIRUSE);
                    break;
                case "RestrictedByQuality":
                    newRestrictions.put(parts[0], RESTRICTED_QUALITY);
                    break;
                case "RestrictedByTbrc":
                    newRestrictions.put(parts[0], RESTRICTED_TBRC);
                    break;
                case "RestrictedInChina":
                    newRestrictions.put(parts[0], RESTRICTED_CHINA);
                    break;
                case "RestrictedSealed":
                    newRestrictions.put(parts[0], RESTRICTED_SEALED);
                    break;
                case "Open":
                    newRestrictions.put(parts[0], RESTRICTED_OPEN);
                    break;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public static void writeToIndex(String titleOrName, String id, String type) {
        if (titleOrName == null || titleOrName.isEmpty()) return;
        Map<String, List<String>> index = (type.equals("outline")) ? outlineIndex : textIndex;
        List<String> ridList = index.get(titleOrName);
        if (ridList == null) {
            ridList = new ArrayList<String>();
            index.put(titleOrName, ridList);
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
            model.read(fStream, null, "TTL") ;
        } catch (RiotException e) {
            System.err.println(f.getAbsolutePath());
            e.printStackTrace();
            return null;
        }
        return model;
    }

    public static String getFullUrlFromBaseFileName(String baseName, String type) {
        return BDR+baseName;
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
    
    public static void fillLocations(Model m, Resource r, ObjectNode rootNode) {
        Property locationProp = m.getProperty(BDO+"workLocation");
        Statement s = r.getProperty(locationProp);
        if (s == null) return;
        Resource location = s.getResource();
        Statement pageStatement = location.getProperty(m.getProperty(BDO, "workLocationPage"));
        if (pageStatement == null) return;
        int beginPage = 0;
        try {
            beginPage = pageStatement.getInt();
        } catch (Exception e) {
            //System.err.println(pageStatement);
            //System.err.println(e.getMessage());
            return;
        }
        Statement volumeStatement = location.getProperty(m.getProperty(BDO, "workLocationVolume"));
        if (volumeStatement == null) return;
        int volume;
        try {
            volume = volumeStatement.getInt();
        } catch (Exception e) {
            //System.err.println(volumeStatement);
            return;
        }
        pageStatement = location.getProperty(m.getProperty(BDO, "workLocationEndPage"));
        if (pageStatement == null) return;
        int endPage;
        try {
            endPage = pageStatement.getInt();
        } catch (Exception e) {
            //System.err.println(pageStatement);
            //System.err.println(e.getMessage());
            return;
        }
        volumeStatement = location.getProperty(m.getProperty(BDO, "workLocationEndVolume"));
        int volumeEnd;
        if (volumeStatement == null) {
            volumeEnd = volume;
        } else {
            try {
                volumeEnd = volumeStatement.getInt();
            } catch (Exception e) {
                return;
            }
        }
        if (volumeEnd != volume) {
        //    System.err.println("at node "+r+" : cross volume locations");
            rootNode.put("volumeEnd", volumeEnd);
        }
        rootNode.put("beginsAt", beginPage);
        rootNode.put("volume", volume);
        rootNode.put("endsAt", endPage);
    }

    public static void fillTreeProperties(Model m, Resource r, ObjectNode rootNode, String type, String baseName, String rootBaseName) {
        if (type.equals("person")) {
            String queryString = "PREFIX bdo: <"+BDO+">\n"
                    + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                    + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                    + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
                    + "SELECT ?eventType ?circa\n"
                    + "WHERE {\n"
                    + "  {\n"
                    + "     ?b rdf:type bdo:PersonEventDeath .\n"
                    + "     ?b bdo:onOrAbout ?circa .\n"
                    + "     BIND (\"death\" AS ?eventType)\n"
                    + "  } UNION {\n"
                    + "     ?b rdf:type bdo:PersonEventBirth .\n"
                    + "     ?b bdo:onOrAbout ?circa .\n"
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
        } else if (type.equals("work")) {
            Property p = m.getProperty(BDO, "workHasPart");
            StmtIterator propIter = r.listProperties(p);
            while(propIter.hasNext()) {
                Statement s = propIter.nextStatement();
                Resource o = s.getResource();
                String oid = o.getProperty(m.getProperty(ADM, "workLegacyNode")).getString();
                ArrayNode a = (ArrayNode) rootNode.get("nodes");
                if (a == null) {
                    a = om.createArrayNode();
                    rootNode.set("nodes", a);
                }
                ObjectNode nodeNode = om.createObjectNode();
                nodeNode.put("id", oid);
                fillLocations(m, o, nodeNode);
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
        Property p = m.getProperty(SKOS_PREFIX+"prefLabel");
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

    public static String getStatus(Model m, Resource r) {
        Property p = m.getProperty(ADM+"status");
        StmtIterator propIter = r.listProperties(p);
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            return s.getResource().getLocalName(); // TODO: mapping?
        }
        return "";
    }
    
    public static void fillResourceInNode(Model m, Resource r, String rName, ObjectNode currentNode, ObjectNode rootNode, String rootName, String type) {
        String label = null;
        StmtIterator propIter = r.listProperties();
        String outlineId = null;
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            Property p = s.getPredicate();
            String pBaseName = p.getLocalName();
            PropInfo pInfo = propMapping.get(pBaseName);
            if (pBaseName.equals("outline")) {
                Resource o = s.getResource();
                outlineId = o.getProperty(m.getProperty(ADM, "workLegacyNode")).getString();
                rootNode.put("outlinedBy", outlineId);
                continue;
            }
            if (pInfo == null) continue;
            if (pInfo.isObjectProp) {
                Resource o = s.getResource();
                if (pInfo.isSimpleObjectProp) {
                    Literal l = o.getProperty(RDFS.label).getLiteral();
                    String lang = l.getLanguage();
                    String ls = l.getString();
                    if (lang.equals("bo-x-ewts"))
                        ls = converter.toUnicode(ls);
                    else if (!lang.equals("bo"))
                        continue;
                    addToOutput(currentNode, pInfo, ls);
                    if (pInfo.toIndex) {
                        if (!rName.equals(rootName))
                            writeToIndex(ls, rootName+'-'+rName, "outline");
                        else
                            writeToIndex(ls, rootName, type);
                    }
                } else {
                    String oid = o.getLocalName();
                    addToOutput(currentNode, pInfo, oid);
                    if (pInfo.mappedProp.equals("hasCreator")) {
                        addAuthorMapping(rName, oid);
                    }
                }
            } else {
//                if (type.equals("outline") && rootName.equals(rName) /* && (pInfo.mappedProp.equals("name") || pInfo.mappedProp.equals("title"))*/) {
//                    // not interested in outline's main title, should be the same as the work
//                    continue;
//                }
//                if (type.equals("outline") && pInfo.mappedProp.equals("name")) {
//                    // not interested in outline names, just titles
//                    continue;
//                }
                if (!type.equals("work") && pInfo.mappedProp.equals("status")) {
                    // only interested in work status
                    continue;
                }
                Literal l = s.getLiteral();
                // TODO: handle non-strings?
                String lang = l.getLanguage();
                if (lang.isEmpty() || (lang.equals("en") && (pInfo.mappedProp.equals("publisherName")||(pInfo.mappedProp.equals("publisherLocation"))))) {
                    addToOutput(currentNode, pInfo, l.getString());
                } else if (lang.equals("bo-x-ewts")) {
                    String uniString = converter.toUnicode(l.getString());
                    if (label != null && label.equals(uniString)) {
                        continue;
                    }
                    if (pInfo.toIndex) {
                        if (!rName.equals(rootName))
                            writeToIndex(uniString, rootName+'-'+rName, "outline");
                        else
                            writeToIndex(uniString, rootName, type);
                    }
                    addToOutput(currentNode, pInfo, uniString);
                    //if (type == "outline") break; // just one title per outline
                }
            }
        }
        if (outlineId != null) {
            rootName = outlineId;
        }
        fillTreeProperties(m, r, rootNode, type, rName, rootName);
    }
    
    private static void fillVolumes(Model m, Resource r, String mainResourceName) {
        String volumesId = r.getLocalName();
        String workId = 'W'+volumesId.substring(1);
        workId = workId.substring(0, workId.indexOf('_'));
        Property p = m.getProperty(BDO, "itemHasVolume");
        StmtIterator propIter = r.listProperties(p);
        ArrayNode volumesNode = om.createArrayNode();
        while(propIter.hasNext()) {
            Statement s = propIter.nextStatement();
            Resource o = s.getResource();
            String imageGroupId = o.getProperty(m.getProperty(ADM, "legacyImageGroupRID")).getString();
            ObjectNode volumeNode = om.createObjectNode();
            volumeNode.put("id", imageGroupId);
            int volnum = o.getProperty(m.getProperty(BDO, "volumeNumber")).getInt();
            Statement totalPag = o.getProperty(m.getProperty(BDO, "imageCount"));
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
        if (!fileName.endsWith(".ttl")) return;
        if (fileName.contains("TLM")) return; // no TLM things
        if (fileName.contains("FPL")) return; // no FPL works (yet)
        if (type.equals("item") && !fileName.contains("_I")) return;
        String baseName = fileName.substring(0, fileName.length()-4);
        if (newRestrictions.containsKey(baseName)) return;
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
        if (!type.equals("item") && !getStatus(m, mainR).equals("StatusReleased")) return; // we keep non-released works?
        if (type.equals("work") || type.equals("outline")) {
            Resource accessR = mainR.getPropertyResourceValue(m.getProperty(ADM, "access"));
            if (accessR == null) {
                System.err.println("no access property on "+mainResourceName);
                return;
            }
            String access = mainR.getPropertyResourceValue(m.getProperty(ADM, "access")).getLocalName();
            if ((!access.equals("AccessOpen")) && !access.equals("AccessFairUse")) return;
        }
        if (type.equals("item")) {
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
        if (type.equals("work")) {
            nodeList = new ArrayList<String>();
        }
        fillResourceInNode(m, mainR, baseName, output, output, baseName, type);
        ObjectNode outline = null;
        if (type.equals("work")) {
            outline = splitOutline(output, baseName);
            if (outline != null) {
                String outlineId = output.get("outlinedBy").asText();
                String outlineFileName = OUTPUT_DIR+"outlines/"+outlineId+".json";
                try {
                    om.writeValue(new File(outlineFileName), outline);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            om.writeValue(new File(outfileName), output);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // System.exit(0);
    }

    private static ObjectNode splitOutline(ObjectNode output, String workId) {
        if (!output.hasNonNull("outlinedBy"))
            return null;
        ObjectNode res = om.createObjectNode();
        res.put("isOutlineOf", workId);
        res.set("nodes", output.get("nodes"));
        output.remove("nodes");
        return res;
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
        Map<String, List<String>> index = (type.equals("outline")) ? outlineIndex : textIndex;
        for (Entry<String, List<String>> entry : index.entrySet()) {
            total = total + 1;
            currentChunk.put(entry.getKey(), entry.getValue());
            if (total > INDEX_LIMIT_SIZE) {
                dumpMapInFile(currentChunk, type+"Index-"+i+".json");
                i = i + 1;
                total = 0;
                currentChunk = new HashMap<String,List<String>>();
            }
        }
        dumpMapInFile(currentChunk, type+"Index-"+i+".json");
        textIndex = new HashMap<String, List<String>>();
    }
    
    public static void migrateOneDir(File dir, String type) {
        File[] files = dir.listFiles();
        Stream.of(files).forEach(file -> migrateOneFile(file, type));
    }
    
    public static void migrateType(String type) {
        textIndex = new HashMap<String, List<String>>();
        createDirIfNotExists(OUTPUT_DIR+type+"s");
        String dirName = DATA_DIR+type+"s";
        File[] files = new File(dirName).listFiles();
        //System.out.println("converting "+files.length+" "+type+" files");
        //Stream.of(files).parallel().forEach(file -> migrateOneFile(file, type));
        Stream.of(files).forEach(file -> migrateOneDir(file, type));
        if (!type.equals("item"))
            dumpIndex(type);
        if (type.equals("work"))
            dumpIndex("outline");
    }

    public static void main( String[] args )
    {
        createDirIfNotExists(OUTPUT_DIR);
        createDirIfNotExists(OUTPUT_DIR+"outlines");
        long startTime = System.currentTimeMillis();
        migrateType("item");
        migrateType("work");
        migrateType("person");
//        migrateOneFile(new File("../xmltoldmigration/tbrc-ttl/items/4d/I12827_I001.ttl"), "item");
//        migrateOneFile(new File("../xmltoldmigration/tbrc-ttl/works/d3/W12827.ttl"), "work");
//        migrateOneFile(new File("../xmltoldmigration/tbrc-ttl/persons/fa/P1583.ttl"), "person");
//        dumpIndex("test");
//        System.out.println("dumping "+outlineWorkTitleMap.size()+" entries to outlineWorkTitle.json");
//        try {
//            om.writeValue(new File(OUTPUT_DIR+"outlineWorkTitle.json"), outlineWorkTitleMap);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("done in "+estimatedTime+" ms");
    }
}
