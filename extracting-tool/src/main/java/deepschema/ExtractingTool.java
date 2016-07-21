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

import static deepschema.Parameters.classes;
import static deepschema.Parameters.classesStream;
import static deepschema.Parameters.datamodelConverter;
import static deepschema.Parameters.includeInstances;
import static deepschema.Parameters.instanceOfRelationsStream;
import static deepschema.Parameters.instances;
import static deepschema.Parameters.instancesStream;
import static deepschema.Parameters.jsonSerializer;
import static deepschema.Parameters.jsonStream;
import static deepschema.Parameters.operation;
import static deepschema.Parameters.output;
import static deepschema.Parameters.rdfSerializer;
import static deepschema.Parameters.rdfStream;
import static deepschema.Parameters.separator;
import static deepschema.Parameters.subclassOfRelationsStream;
import static deepschema.Parameters.txtStream;
import static deepschema.Parameters.useCache;
import static deepschema.DumpOperations.enhanceAndFilter;
import static deepschema.DumpOperations.exploreDataset;
import static deepschema.DumpOperations.processEntitiesFromWikidataDump;
import static deepschema.DumpOperations.readFromDump;
import static deepschema.DumpOperations.structureOutput;

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
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.openrdf.rio.RDFFormat;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.json.jackson.JacksonObjectFactory;
import org.wikidata.wdtk.datamodel.json.jackson.JsonSerializer;
import org.wikidata.wdtk.dumpfiles.DumpProcessingController;
import org.wikidata.wdtk.rdf.PropertyRegister;
import org.wikidata.wdtk.rdf.RdfSerializer;

import deepschema.Parameters.Operation;
import deepschema.Parameters.Output;
import deepschema.Parameters.WikidataClassProperties;
import deepschema.Parameters.WikidataInstanceProperties;

/**
 * deepchema.org extracting tool.
 *
 * @author Panayiotis Smeros
 *
 */
public class ExtractingTool implements EntityDocumentProcessor {

	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 */
	public ExtractingTool() {
		try {
			if (output == Output.TSV) {

				classesStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("classes.tsv.gz")));
				subclassOfRelationsStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("subclassOfRelations.tsv.gz")));
				if (includeInstances) {
					instancesStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("instances.tsv.gz")));
					instanceOfRelationsStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("instanceOfRelations.tsv.gz")));
				}
			}
			else if (output == Output.JSON) {
				jsonStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("classesAndInstances.json.gz")));

				// DataModel
				datamodelConverter = new DatamodelConverter(new JacksonObjectFactory());
				// Do not copy references.
				datamodelConverter.setOptionDeepCopyReferences(false);
				// Only copy English labels, descriptions, and aliases.
				datamodelConverter.setOptionLanguageFilter(Collections.singleton("en"));
				// Copy statements of all the properties.
				datamodelConverter.setOptionPropertyFilter(null);
				// Do not copy sitelinks.
				datamodelConverter.setOptionSiteLinkFilter(Collections.<String> emptySet());

				jsonSerializer = new JsonSerializer(jsonStream);
				jsonSerializer.open();
			}
			else if (output == Output.RDF) {
				rdfStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("classesAndInstances.nt.gz")));

				// DataModel
				datamodelConverter = new DatamodelConverter(new JacksonObjectFactory());
				// Do not copy references.
				datamodelConverter.setOptionDeepCopyReferences(false);
				// Only copy English labels, descriptions, and aliases.
				datamodelConverter.setOptionLanguageFilter(Collections.singleton("en"));
				// Copy statements of all the properties.
				datamodelConverter.setOptionPropertyFilter(null);
				// Do not copy sitelinks.
				datamodelConverter.setOptionSiteLinkFilter(Collections.<String> emptySet());

				rdfSerializer = new RdfSerializer(RDFFormat.NTRIPLES, rdfStream, new DumpProcessingController("wikidatawiki").getSitesInformation(), PropertyRegister.getWikidataPropertyRegister());
				// Serialize simple statements (and nothing else) for all items
				rdfSerializer.setTasks(RdfSerializer.TASK_ITEMS | RdfSerializer.TASK_SIMPLE_STATEMENTS);
				rdfSerializer.open();
			}
			else if (output == Output.TXT) {
				txtStream = new GzipCompressorOutputStream(new BufferedOutputStream(openOutput("output.txt.gz")));
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initializes the extracting procedure.
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

		ExtractingTool extractingTool = new ExtractingTool();

		if (useCache) {
			extractingTool.cache("read");
			extractingTool.writeOutput();
		}
		else {
			operation = Operation.READ_FROM_DUMP;
			processEntitiesFromWikidataDump(extractingTool);
			operation = Operation.ENHANCE_AND_FILTER;
			processEntitiesFromWikidataDump(extractingTool);
			extractingTool.writeOutput();
			extractingTool.cache("write");
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
			if (output == Output.TSV) {
				for (Entry<String, WikidataClassProperties> entry : classes.entrySet())
					classesStream.write((entry.getKey().substring(1) + separator + entry.getValue().label + "\n").getBytes());

				for (Entry<String, WikidataClassProperties> entry : classes.entrySet())
					for (String value : entry.getValue().subclassOf)
						subclassOfRelationsStream.write((entry.getKey().substring(1) + separator + value.substring(1) + "\n").getBytes());

				if (includeInstances) {
					for (Entry<String, WikidataInstanceProperties> entry : instances.entrySet())
						instancesStream.write((entry.getKey().substring(1) + separator + entry.getValue().label + "\n").getBytes());

					for (Entry<String, WikidataInstanceProperties> entry : instances.entrySet())
						for (String value : entry.getValue().instanceOf)
							instanceOfRelationsStream.write((entry.getKey().substring(1) + separator + value.substring(1) + "\n").getBytes());
				}
				classesStream.close();
				subclassOfRelationsStream.close();

				if (includeInstances) {
					instancesStream.close();
					instanceOfRelationsStream.close();
				}
			}
			else if (output == Output.JSON) {
				operation = Operation.STRUCTURE_OUTPUT;
				processEntitiesFromWikidataDump(this);
				jsonStream.close();
			}
			else if (output == Output.RDF) {
				operation = Operation.STRUCTURE_OUTPUT;
				processEntitiesFromWikidataDump(this);
				rdfStream.close();
			}
			else if (output == Output.TXT) {
				operation = Operation.EXPLORE_DATASET;
				processEntitiesFromWikidataDump(this);
				txtStream.close();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Caches classes and subclasses to file.
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
					classes = (Map<String, WikidataClassProperties>) ((ObjectInputStream) objectInputStream).readObject();
					instances = (Map<String, WikidataInstanceProperties>) ((ObjectInputStream) objectInputStream).readObject();
					objectInputStream.close();
				}
				case "write": {
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(cacheFile));
					objectOutputStream.writeObject(classes);
					objectOutputStream.writeObject(instances);
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
		if (operation == Operation.READ_FROM_DUMP) {
			readFromDump(itemDocument);
		}
		else if (operation == Operation.ENHANCE_AND_FILTER) {
			enhanceAndFilter(itemDocument);
		}
		else if (operation == Operation.STRUCTURE_OUTPUT) {
			structureOutput(itemDocument);
		}
		else if (operation == Operation.EXPLORE_DATASET) {
			exploreDataset(itemDocument);
		}
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		// Do not serialize any properties.
	}
}
