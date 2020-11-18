package ddg.mongodbapp;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GenerateDocument {


    public static void main(String[] arg) {

        /**** Read from CSV ****/
        String highwayFile = "C:/Users/podil/Downloads/highways.csv";
        String stationsFile = "C:/Users/podil/Downloads/freeway_stations.csv";
        String detectorsFile = "C:/Users/podil/Downloads/freeway_detectors.csv";
        //String loopDataFile = "C:/Users/podil/Downloads/freeway_loopdata.csv";


        BufferedReader brh = null;
        BufferedReader brs = null;
        BufferedReader brd = null;
        BufferedReader br = null;
        String hl = "";
        String sl = "";
        String dl = "";
        String loop = "";
        String cvsSplitBy = ",";

        String uriString = "mongodb://104.154.208.103:" + 27017;

        // Creating a Mongo client

        MongoClientURI uri = new MongoClientURI(uriString);
        MongoClient mongo = new MongoClient(uri);
        MongoDatabase database = mongo.getDatabase("config");
        MongoIterable<String> mongoIterable = database.listCollectionNames();
        MongoCursor<String> iterator = mongoIterable.iterator();
        // Creating Credentials
        MongoCredential credential;
        //credential = MongoCredential.createCredential("sampleUser", "myDb", "password".toCharArray());
        System.out.println("Connected to the database successfully");

        // Retrieving collection highways
        MongoCollection<Document> collection = database.getCollection("stationcollection");
        collection.deleteMany(new Document());
        // read CSV data
        try {

            Document highway_document;
            Document station_document = null;
            Document detector_document;
            Document loop_document;

            brs = new BufferedReader(new FileReader(stationsFile));
            while ((sl = brs.readLine()) != null) {
                String[] station = sl.split(cvsSplitBy);
                String stationid = station[0];
                String highwayid = station[1];
                if (station[1] != null && !station[1].isEmpty()) {
                    station_document = new Document("stationid", station[0]).append("highwayid", station[1])
                            .append("milepost", station[2]).append("locationtext", station[3])
                            .append("upstream", station[4]).append("downstream", station[5])
                            .append("stationclass", station[6]).append("numberlanes", station[7])
                            .append("latlon", station[8]).append("length", station[9]);

                    brd = new BufferedReader(new FileReader(detectorsFile));
                    List<Document> detectordocuments = new ArrayList<Document>();
                    while ((dl = brd.readLine()) != null) {
                        String[] detector = dl.split(cvsSplitBy);
                        if (detector[6] != null && !detector[6].isEmpty()
                                && detector[6].equalsIgnoreCase(stationid)) {
                            detector_document = new Document("detectorid", detector[0])
                                    .append("highwayid", detector[1]).append("milepost", detector[2])
                                    .append("locationtext", detector[3]).append("detectorclass", detector[4])
                                    .append("lanenumber", detector[5]).append("stationid", detector[6]);
                            detectordocuments.add(detector_document);
                        }
                    }
                    station_document.append("detectors", detectordocuments);
                    brh = new BufferedReader(new FileReader(highwayFile));
                    while ((hl = brh.readLine()) != null) {
                        String[] highway = hl.split(cvsSplitBy);
                        if (highway[0] != null && !highway[0].isEmpty() && highway[0].equalsIgnoreCase(highwayid)) {
                            station_document.append("shortdirection", highway[1])
                                    .append("direction", highway[2]).append("highwayname", highway[3]);
                        }
                    }


                }
                collection.insertOne(station_document);
            }

            System.out.println("Execution finished!!");

        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        } catch (
                IOException e) {
            e.printStackTrace();
        } finally {
            if (brh != null) {
                try {
                    brh.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (brs != null) {
                try {
                    brs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (brd != null) {
                try {
                    brd.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

