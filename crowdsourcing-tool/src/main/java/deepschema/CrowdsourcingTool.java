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

import static deepschema.DumpOperations.*;
import static deepschema.Parameters.*;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;

import deepschema.Parameters.CrowdsourcingInfo;

/**
 * deepchema.org extracting tool.
 *
 * @author Panayiotis Smeros
 *
 */
public class CrowdsourcingTool implements EntityDocumentProcessor {

	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 */
	public CrowdsourcingTool() {
		try {
			outputStream = new BufferedOutputStream(openOutput("output.tsv"));
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initializes the procedure.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Create the appender that will write log messages to the console.
		ConsoleAppender consoleAppender = new ConsoleAppender();
		// Define the pattern of log messages.
		// Insert the string "%c{1}:%L" to also show class name and line.
		String pattern = "%d{yyyy-MM-dd HH:mm:ss} %-5p - %m%n";
		consoleAppender.setLayout(new PatternLayout(pattern));
		// Change to Level.ERROR for fewer messages:
		consoleAppender.setThreshold(Level.INFO);
		consoleAppender.activateOptions();
		Logger.getRootLogger().addAppender(consoleAppender);

		CrowdsourcingTool crowdsourcingTool = new CrowdsourcingTool();

		if (useCache) {
			crowdsourcingTool.cache("read");
			crowdsourcingTool.writeOutput();
		}
		else {
			readGluing();
			getSchemaInfo();
			processEntitiesFromWikidataDump(crowdsourcingTool);
			crowdsourcingTool.writeOutput();
			crowdsourcingTool.cache("write");
		}
	}

	/**
	 * Opens output file.
	 * 
	 * @param filename
	 * @return FileOutputStream
	 */
	FileOutputStream openOutput(String filename) throws IOException {
		Path directoryPath = Paths.get("results");

		try {
			Files.createDirectory(directoryPath);
		}
		catch (FileAlreadyExistsException e) {
			if (!Files.isDirectory(directoryPath)) {
				throw e;
			}
		}

		Path filePath = directoryPath.resolve(filename);
		return new FileOutputStream(filePath.toFile());
	}

	/**
	 * Writes output to the corresponding files.
	 */
	void writeOutput() {
		try {

			outputStream.write(("WikidataLabel" + separator + "SchemaLabel" + separator + "WikidataDescription" + separator + "SchemaDescription" + separator + "WikidataURL" + separator + "SchemaURL" + separator + "relation" + "\n").getBytes());

			for (int index = 0; index < crowdsourcingInfoList.size(); index++) {

				CrowdsourcingInfo crowdsourcingInfo = crowdsourcingInfoList.get(index);

				// TODO:remove
				if (crowdsourcingInfo.WikidataLabel == null)
					continue;

				outputStream.write((crowdsourcingInfo.WikidataLabel + separator + crowdsourcingInfo.SchemaLabel + separator + crowdsourcingInfo.WikidataDescription + separator + crowdsourcingInfo.SchemaDescription + separator + crowdsourcingInfo.WikidataURL + separator + crowdsourcingInfo.SchemaURL + separator + crowdsourcingInfo.relation + "\n").getBytes());
			}

			outputStream.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Caches Crowdsourcing Info to file.
	 * 
	 * @param operation : "read" or "write"
	 */
	@SuppressWarnings("unchecked")
	void cache(String action) {
		final String cacheFile = ".cache";
		try {
			switch (action) {
				case "read": {
					ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(cacheFile));
					crowdsourcingInfoList = (List<CrowdsourcingInfo>) ((ObjectInputStream) objectInputStream).readObject();
					objectInputStream.close();
				}
				case "write": {
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(cacheFile));
					objectOutputStream.writeObject(crowdsourcingInfoList);
					objectOutputStream.flush();
					objectOutputStream.close();
				}
			}
		}
		catch (ClassNotFoundException | IOException e) {
			System.err.println("Problem while reading/writing from/to cache.");
			e.printStackTrace();
		}
	}

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		getWikidataInfo(itemDocument);
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		// Do not serialize any properties.
	}
}
