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
import java.util.stream.Stream;

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
    public static final String WORK_PREFIX = "http://purl.bdrc.io/ontology/work/";
    public static final EwtsConverter converter = new EwtsConverter();

    // extract tbrc/ folder of exist-db backup here:
    public static final String DATA_DIR = "../xmltoldmigration/tbrc-jsonld/";
    public static final String OUTPUT_DIR = "tbrc-bfa/";

    public static final Map<String, List<String>> textIndex = new HashMap<String, List<String>>();
    public static final Map<String, List<String>> authorTextMap = new HashMap<String, List<String>>();

    public static final ObjectMapper om = new ObjectMapper();

    public static final Map<String, PropInfo> propMapping = new HashMap<String,PropInfo>();

    static {
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
        if (titleOrName.isEmpty()) return;
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
            e.printStackTrace();
            return null;
        }
        return model;
    }

    public static String getFullUrlFromBaseFileName(String baseName, String type) {
        switch(type) {
        case "work":
            return WORK_PREFIX+baseName;
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

    public static void migrateOneFile(File file, String type) {
        if (file.isDirectory()) return;
        String fileName = file.getName();
        if (!fileName.endsWith(".jsonld")) return;
        String baseName = fileName.substring(0, fileName.length()-7);
        ObjectNode output = om.createObjectNode();
        String outfileName = baseName+".json";
        outfileName = OUTPUT_DIR+type+"s/"+outfileName;
        Model m = modelFromFile(file);
        if (m == null) return;
        String mainResourceName = getFullUrlFromBaseFileName(baseName, type);
        Resource mainR = m.getResource(mainResourceName);
        if (mainR == null) {
            System.err.println("unable to find resource "+mainResourceName);
            return;
        }
        StmtIterator propIter = mainR.listProperties();
        if (type == "person") {
            List<String> workList = authorTextMap.get(baseName);
            if (workList == null) return; // if a person didn't write books, we just skip
            ArrayNode a = om.valueToTree(workList);
            output.set("creatorOf", a);
        }
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
                addToOutput(output, pInfo, oid);
                if (pInfo.mappedProp.equals("hasCreator")) {
                    addAuthorMapping(baseName, oid);
                }
            } else {
                Literal l = s.getLiteral();
                // TODO: handle non-strings?
                String lang = l.getLanguage();
                if (lang.isEmpty()) {
                    addToOutput(output, pInfo, l.getString());
                } else if (lang.equals("bo-x-ewts")) {
                    String uniString = converter.toUnicode(l.getString());
                    writeToIndex(uniString, baseName, type);
                    addToOutput(output, pInfo, uniString);
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

    public static void migrateType(String type) {
        createDirIfNotExists(OUTPUT_DIR+type+"s");
        String dirName = DATA_DIR+"tbrc-"+type+"s";
        File[] files = new File(dirName).listFiles();
        System.out.println("converting "+files.length+" "+type+" files");
        //Stream.of(files).parallel().forEach(file -> migrateOneFile(file, type));
        Stream.of(files).forEach(file -> migrateOneFile(file, type));
    }

    public static void main( String[] args )
    {
        createDirIfNotExists(OUTPUT_DIR);
        long startTime = System.currentTimeMillis();
        migrateType("work");
        migrateType("person");
        //        migrateType("outline");
        createDirIfNotExists(OUTPUT_DIR+"works");
        //migrateOneFile(new File("src/test/resources/W12827.jsonld"), "work");
        //        migrateOneFile(new File("src/test/resources/P1583.jsonld"), "person");
        try {
            om.writeValue(new File(OUTPUT_DIR+"textIndex.json"), textIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("done in "+estimatedTime+" ms");
    }
}
