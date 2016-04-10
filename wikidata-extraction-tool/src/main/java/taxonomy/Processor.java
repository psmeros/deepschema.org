package taxonomy;

/*
 * #%L
 * Wikidata Toolkit Examples
 * %%
 * Copyright (C) 2014 - 2015 Wikidata Toolkit Developers
 * %%
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
 * #L%
 */

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.Reference;
import org.wikidata.wdtk.datamodel.interfaces.Snak;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.json.jackson.JacksonObjectFactory;
import org.wikidata.wdtk.datamodel.json.jackson.JsonSerializer;

/**
 * This example illustrates how to create a taxonomy serialization of the
 * data found in a dump.
 *
 * @author Markus Kroetzsch
 * @author Panayiotis Smeros
 *
 */
public class Processor implements EntityDocumentProcessor {

	OutputStream classesStream, subClassesStream, jsonStream;

	enum Operation {READ, ENCHANCE, WRITE}
	public Operation operation;

	enum Output {JSON, TSV}
	final String separator = "\t";


	DatamodelConverter datamodelConverter;
	JsonSerializer jsonSerializer;

	Map <Pair <String, String>, Integer> classes;
	Set <Pair <String, String>> subclasses;

	//Parameters
	public Output output = Output.JSON;

	public Boolean useCache = true;

	public Boolean filterCategories = false;

	public Boolean filterDiseaseOntology = true;

	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 * @throws IOException (if there is a problem opening the output file(s))
	 */
	public Processor() throws IOException {

		this.classes = new HashMap<>();
		this.subclasses = new HashSet<>();

		if (output == Output.TSV)	{
			this.classesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedClasses.tsv.gz")));
			this.subClassesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedSubClasses.tsv.gz")));
		}
		else if (output == Output.JSON) {
			this.jsonStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedClasses.json.gz")));

			//DataModel
			this.datamodelConverter = new DatamodelConverter(new JacksonObjectFactory());
			// Do not copy references.
			this.datamodelConverter.setOptionDeepCopyReferences(false);
			// Only copy English labels, descriptions, and aliases.
			this.datamodelConverter.setOptionLanguageFilter(Collections.singleton("en"));
			// Copy statements of all the properties.
			this.datamodelConverter.setOptionPropertyFilter(null);
			// Do not copy sitelinks.
			this.datamodelConverter.setOptionSiteLinkFilter(Collections.<String> emptySet());

			this.jsonSerializer = new JsonSerializer(jsonStream);
			this.jsonSerializer.open();
		}
	}

	/**
	 * Runs the example program.
	 *
	 * @param args
	 * @throws IOException (if there was a problem in writing the output file)
	 */
	public static void main(String[] args) throws IOException {
		ExampleHelpers.configureLogging();

		Processor taxonomyProcessor = new Processor();
		if (!taxonomyProcessor.useCache) {
			taxonomyProcessor.operation = Operation.READ;
			ExampleHelpers.processEntitiesFromWikidataDump(taxonomyProcessor);
			taxonomyProcessor.operation = Operation.ENCHANCE;
			ExampleHelpers.processEntitiesFromWikidataDump(taxonomyProcessor);
			if (taxonomyProcessor.output== Output.JSON) {
				taxonomyProcessor.operation = Operation.WRITE;
				ExampleHelpers.processEntitiesFromWikidataDump(taxonomyProcessor);			
			}
			else
				taxonomyProcessor.writeToStreams();
			taxonomyProcessor.caching("write");
		}
		else {
			taxonomyProcessor.caching("read");
			if (taxonomyProcessor.output== Output.JSON) {
				taxonomyProcessor.operation = Operation.WRITE;
				ExampleHelpers.processEntitiesFromWikidataDump(taxonomyProcessor);			
			}
			else
				taxonomyProcessor.writeToStreams();
		}

		taxonomyProcessor.close();
	}	

	/**
	 * Writes classes and subclasses to file.
	 * 
	 * @param operation: "read" or "write"
	 */		
	public void writeToStreams() {
		try {
			for (Entry<Pair<String, String>, Integer> entry : this.classes.entrySet())
				classesStream.write((entry.getKey().getLeft()+separator+entry.getKey().getRight()+separator+entry.getValue()+"\n").getBytes());

			for (Pair<String, String> entry : subclasses)
				subClassesStream.write((entry.getLeft()+separator+entry.getRight()+"\n").getBytes());					 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Caches classes and subclasses to file.
	 * 
	 * @param operation: "read" or "write"
	 */	
	@SuppressWarnings("unchecked")
	public void caching(String operation) {		
		final String cacheFile=".cache";
		try {
			switch (operation) {	
			case "read" : {
				ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(cacheFile));
				classes = (Map<Pair<String, String>, Integer>) ((ObjectInputStream) objectInputStream).readObject();
				subclasses = (Set<Pair<String, String>>) ((ObjectInputStream) objectInputStream).readObject();
				objectInputStream.close();
			}
			case "write" : {
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(cacheFile));
				objectOutputStream.writeObject(classes);
				objectOutputStream.writeObject(subclasses);
				objectOutputStream.flush();
				objectOutputStream.close();
			}
			}
		} catch (ClassNotFoundException | IOException e) {
			System.err.println("Problem while reading/writing from/to cache.");
			e.printStackTrace();
		}
	}	

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		if (operation == Operation.READ) {

			StatementGroup sg = null;

			//instanceOf(I, C) => Class(C) #RDFS
			sg = itemDocument.findStatementGroup("P31");

			if (sg != null) {
				for (Statement s : sg.getStatements()) {			
					ItemIdValue value = (ItemIdValue) s.getValue();
					if (value != null)
						if(!this.classes.containsKey(Pair.of(value.getId(), "")))
							this.classes.put(Pair.of(value.getId(), ""), 1);
						else
							this.classes.put(Pair.of(value.getId(), ""), this.classes.get(Pair.of(value.getId(), "")) + 1);
				}				
			}

			//subclassOf(C1, C2) => Class(C1) /\ Class(C2) #RDFS
			sg = itemDocument.findStatementGroup("P279");

			if (sg != null) {
				if(!this.classes.containsKey(Pair.of(sg.getSubject().getId(), "")))
					this.classes.put(Pair.of(sg.getSubject().getId(), ""), 0);

				for (Statement s : sg.getStatements()) {

					//filter relations from Disease Ontology.
					if (filterDiseaseOntology && !s.getReferences().isEmpty()) {
						Iterator<? extends Reference> it = s.getReferences().iterator();
						while (it.hasNext()) {
							Iterator<Snak> sn = it.next().getAllSnaks();
							Snak snack;
							while (sn.hasNext()) {
								snack = sn.next();
								if (snack.getPropertyId().getId().equals("P1065") && snack.getValue().toString().contains("DiseaseOntology"))
									return;
							}	
						}
					}

					ItemIdValue value = (ItemIdValue) s.getValue();
					if (value != null) {
						if(!this.classes.containsKey(Pair.of(value.getId(), "")))
							this.classes.put(Pair.of(value.getId(), ""), 0);

						subclasses.add(Pair.of(sg.getSubject().getId(), value.getId()));
					}
				}				
			}			
		}
		else if (operation == Operation.ENCHANCE) {
			if(classes.containsKey(Pair.of(itemDocument.getEntityId().getId(), ""))) {

				//Add english label; if not exists, add the first available.
				String label = itemDocument.findLabel("en");
				if (label == null) {
					Collection<MonolingualTextValue> otherlabels = itemDocument.getLabels().values();
					if (otherlabels.isEmpty())
						label = "No Label";
					else
						label = otherlabels.iterator().next().getText();
				}
				label = label.replace(separator, " ");

				//filter category classes
				if (filterCategories && ((label.startsWith("Cat") || label.startsWith("Кат")) && label.contains(":"))) {
					classes.remove(Pair.of(itemDocument.getEntityId().getId(), ""));
					return;
				}							

				classes.put(Pair.of(itemDocument.getEntityId().getId(), label), classes.remove(Pair.of(itemDocument.getEntityId().getId(), "")));
			}
		}
		else if (operation == Operation.WRITE && output == Output.JSON) {
			if(classes.containsKey(Pair.of(itemDocument.getEntityId().getId(), "")))
				this.jsonSerializer.processItemDocument(this.datamodelConverter.copy(itemDocument));
		}
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		//Do not serialize any properties.
	}

	/**
	 * Closes the output. Should be called after the taxonomy serialization was finished.
	 *
	 * @throws IOException (if there was a problem closing the output)
	 */
	public void close() throws IOException {
		if(output == Output.TSV) {
			this.classesStream.close();
			this.subClassesStream.close();
		}
		else if (output == Output.JSON) {
			this.jsonStream.close();
		}
	}

}
