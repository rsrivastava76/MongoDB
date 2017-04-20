package com.mongo;

import static com.mongodb.client.model.Filters.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Projections.excludeId;

public class MongoDBJDBC {

	public static void main(String[] args) {

		try {

			String user = "mongouser"; // the user name
			String password = "pass@123"; // the password as a character array
			String database = "extended_doc"; // the name of the database in which the user is defined
			String server1 = "test.mongodb.firstrain.com";
			int port = 27017;

			List<ServerAddress> seeds = new ArrayList<ServerAddress>();
			seeds.add( new ServerAddress( server1,port ));

			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add( MongoCredential.createScramSha1Credential( user, database, password.toCharArray() ) );

			MongoClient mongoClient = new MongoClient( seeds, credentials );
			MongoDatabase db = mongoClient.getDatabase(database);

			System.out.println("Connect to DataBase Sucessfully");

			MongoCollection<Document> docs = db.getCollection("meta");

			System.out.println("Total Records in Meta Collections == " + docs.count() );

			Document myDoc = docs.find().first();
			System.out.println(" First Record in Collections " + myDoc.toJson());
			System.out.println("******************************11111111***************************************");

			// now use a query to get 1 document out
			myDoc = docs.find(eq("ng.ngrams.phrase", "supervisors chambers")).first();
			System.out.println("First Record in Collections filter on Phrase 'supervisors chambers' ==" + myDoc.toJson());

			//db.meta.find({"ng.ngrams":{$elemMatch:{"phrase":"trump","firstLocation":"Q1"}}}).limit(1)

			// element search 
			Document  statusQuery = new Document("phrase", "trump").append("firstLocation", "Q1");
			Document elemMatchQuery = new Document("$elemMatch", statusQuery);

			Document fields = new Document();
			fields.put("ng.ngrams", elemMatchQuery);

			System.out.println("Filter Records Count == " + docs.count(fields));

			// Find top 2 results of filter records
			FindIterable<Document> cursor = docs.find(fields).limit(2);
			//int i = 0;

			for (Document document : cursor) {
				System.out.println("Documents = " + document.toJson());
			}

			System.out.println("******************************22222222***************************************");

			// now use a range query to get a larger subset
			MongoCursor<Document> mCursor  = docs.find(gt("imageScore", 1)).iterator();

			try {
				while (mCursor.hasNext()) {
					System.out.println(mCursor.next().toJson());
				}
			} finally {
				mCursor.close();
			}

			System.out.println("******************************33333333***************************************");

			// Sorting
			myDoc = docs.find(exists("docImage")).sort(descending("imageScore")).first();
			System.out.println(myDoc.toJson());

			System.out.println("******************************44444444***************************************");

			// Projection
			myDoc = docs.find().projection(excludeId()).first();
			System.out.println(myDoc.toJson());

			System.out.println("******************************55555555***************************************");

			mongoClient.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
