package deepschema;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static deepschema.Parameters.OFFLINE_MODE;
import static deepschema.Parameters.TIMEOUT_SEC;
import static deepschema.Parameters.crowdsourcingInfoList;
import static deepschema.Parameters.gluingFile;
import static deepschema.Parameters.schemaFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.dumpfiles.DumpContentType;
import org.wikidata.wdtk.dumpfiles.DumpProcessingController;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor.TimeoutException;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;

import deepschema.Parameters.CrowdsourcingInfo;

/**
 * Wikidata dump operations.
 *
 * @author Panayiotis Smeros
 *
 */
public class DumpOperations {

	/**
	 * Reads gluing file.
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws UnsupportedRDFormatException
	 * @throws RDFParseException
	 * @throws RDFHandlerException
	 *
	 */
	public static void readGluing() {

		try {
			
		    Model statements = Rio.parse(new FileInputStream(new File(gluingFile)), "", RDFFormat.NTRIPLES);
			
			for (Statement statement: statements) {
				CrowdsourcingInfo crowdsourcingInfo = new CrowdsourcingInfo();
				crowdsourcingInfo.WikidataURL = statement.getSubject().stringValue();
				crowdsourcingInfo.relation = statement.getPredicate().stringValue();
				crowdsourcingInfo.SchemaURL = statement.getObject().stringValue();
				crowdsourcingInfoList.add(crowdsourcingInfo);
			}
			
			for (int index = 0; index < crowdsourcingInfoList.size(); index++) {
			
				System.out.println(index);
				CrowdsourcingInfo crowdsourcingInfo = crowdsourcingInfoList.get(index);
				crowdsourcingInfo.SchemaLabel = "test1";
				crowdsourcingInfoList.set(index, crowdsourcingInfo);
			}

			for (int index = 0; index < crowdsourcingInfoList.size(); index++) {
				
				CrowdsourcingInfo crowdsourcingInfo = crowdsourcingInfoList.get(index);
				System.out.println(crowdsourcingInfo.SchemaLabel);
			}
			
			
		    statements = Rio.parse(new FileInputStream(new File(schemaFile)), "", RDFFormat.NTRIPLES);
		    for (Statement statement: statements) {
				
				System.out.println(statement.getSubject().stringValue());
			}

			
		}
		catch (RDFParseException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Gets Crowdsourcing Info from dumps.
	 * 
	 * @param ItemDocument
	 */
	public static void getCrowdsourcingInfo(ItemDocument itemDocument) {
		// int wikidataClass = Integer.parseInt(itemDocument.getEntityId().getId().substring(1));
		//
		// if (!classes.containsKey(wikidataClass) || classes.get(wikidataClass).label != null)
		// return;
		//
		// if (filter(itemDocument))
		// classes.remove(wikidataClass);
		// else
		// classes.get(wikidataClass).label = getLabel(itemDocument);
	}

	/**
	 * Processes all entities in a Wikidata dump using the given entity processor.
	 *
	 * @param entityDocumentProcessor
	 * @author Markus Kroetzsch
	 */
	public static void processEntitiesFromWikidataDump(EntityDocumentProcessor entityDocumentProcessor) {

		// Controller object for processing dumps.
		DumpProcessingController dumpProcessingController = new DumpProcessingController("wikidatawiki");
		dumpProcessingController.setOfflineMode(OFFLINE_MODE);

		// // Optional: Use another download directory.
		// dumpProcessingController.setDownloadDirectory(System.getProperty("user.dir"));

		// Subscribe to the most recent entity documents of type wikibase item.
		dumpProcessingController.registerEntityDocumentProcessor(entityDocumentProcessor, null, true);

		// Also add a timer that reports some basic progress information.
		EntityTimerProcessor entityTimerProcessor = new EntityTimerProcessor(TIMEOUT_SEC);
		dumpProcessingController.registerEntityDocumentProcessor(entityTimerProcessor, null, true);

		try {
			// Start processing (may trigger downloads where needed).
			MwDumpFile dumpFile = dumpProcessingController.getMostRecentDump(DumpContentType.JSON);

			if (dumpFile != null)
				dumpProcessingController.processDump(dumpFile);
		}
		catch (TimeoutException e) {
			// The timer caused a time out. Continue and finish normally.
		}

		// Print final timer results.
		entityTimerProcessor.close();
	}

}
