package org.wikidata.wdtk.examples;

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
import java.util.Map;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.StringValue;
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
public class TaxonomyProcessor implements EntityDocumentProcessor {

	OutputStream classesStream, subClassesStream, jsonStream;
	
	enum Operation {FINDCLASSES, EXTRACTJSON, EXTRACTTSV}
	static Operation operation;
	
	Map <String, Integer> classes;

	DatamodelConverter datamodelConverter;
	JsonSerializer jsonSerializer;


	/**
	 * Runs the example program.
	 *
	 * @param args
	 * @throws IOException (if there was a problem in writing the output file)
	 */
	public static void main(String[] args) throws IOException {
		ExampleHelpers.configureLogging();

		TaxonomyProcessor jsonTaxonomyProcessor = new TaxonomyProcessor(Operation.EXTRACTTSV);
		//operation = Operation.FINDCLASSES;
		//ExampleHelpers.processEntitiesFromWikidataDump(jsonTaxonomyProcessor);
		//jsonTaxonomyProcessor.caching("write");

		jsonTaxonomyProcessor.caching("read");
		ExampleHelpers.processEntitiesFromWikidataDump(jsonTaxonomyProcessor);
		
		jsonTaxonomyProcessor.close();
	}	
	
	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 * @param Operation op
	 * @throws IOException (if there is a problem opening the output file(s))
	 */
	public TaxonomyProcessor(Operation op) throws IOException {

		this.classes = new HashMap<>();
		operation = op;

		if (operation == Operation.EXTRACTTSV)	{
			this.classesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedClasses.tsv.gz")));
			this.subClassesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedSubClasses.tsv.gz")));
		}
		else if (operation == Operation.EXTRACTJSON) {
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

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		if (operation == Operation.FINDCLASSES)
		{
			StatementGroup sg = null;

			//instanceOf(I, C) => Class(C) #RDFS
			sg = itemDocument.findStatementGroup("P31");
			
			if (sg != null) {
				for (Statement s : sg.getStatements()) {			
					ItemIdValue value = (ItemIdValue) s.getValue();
					if (value != null)
						if(!this.classes.containsKey(value.getId()))
							this.classes.put(value.getId(), 1);
						else
							this.classes.put(value.getId(), this.classes.get(value.getId()) + 1);
				}				
			}

			//subclassOf(C1, C2) => Class(C1) /\ Class(C2) #RDFS
			sg = itemDocument.findStatementGroup("P279");

			if (sg != null) {
				if(!this.classes.containsKey(sg.getSubject().getId()))
					this.classes.put(sg.getSubject().getId(), 0);
				
				for (Statement s : sg.getStatements()) {
					ItemIdValue value = (ItemIdValue) s.getValue();
					if (value != null)
						if(!this.classes.containsKey(value.getId()))
							this.classes.put(value.getId(), 0); 
				}				
			}			
		}
		else {
			if(classes.containsKey(itemDocument.getEntityId().getId())) {
				if (operation == Operation.EXTRACTTSV){
					final String separator = "\t";
					try {

						//Add english label; if not exists, add another available.
						String label = itemDocument.findLabel("en");
						if (label == null) {
							Collection<MonolingualTextValue> otherlabels = itemDocument.getLabels().values();
							if (otherlabels.isEmpty())
								label = "No Label";
							else
								label = otherlabels.iterator().next().getText();
						}

						StatementGroup sg = null;
						
						//Find equivalent class from schema.org and dbpedia.
						String schemaOrgClass = "";
						String dbpediaOrgClass = "";
						sg = itemDocument.findStatementGroup("P1709");

						if (sg != null) {
							for (Statement s : sg.getStatements()) {
								StringValue value = (StringValue) s.getValue();
								if (value != null)
									if	(value.getString().contains("dbpedia.org"))
										dbpediaOrgClass = value.getString();
									else if (value.getString().contains("schema.org"))
										schemaOrgClass = value.getString();
							}	
						}
						
						this.classesStream.write((itemDocument.getEntityId().getId()+separator+label+separator+dbpediaOrgClass+separator+schemaOrgClass+"\n").getBytes());									
						
						//Find subclass relations.
						sg = itemDocument.findStatementGroup("P279");

						if (sg != null) {
							for (Statement s : sg.getStatements()) {
								ItemIdValue value = (ItemIdValue) s.getValue();
								if (value != null)
									this.subClassesStream.write((itemDocument.getEntityId().getId()+separator+value.getId()+"\n").getBytes());					 
							}	
						}
					} catch (IOException e) {
						e.printStackTrace();
					}								
				}
				else if (operation == Operation.EXTRACTJSON) {
						this.jsonSerializer.processItemDocument(this.datamodelConverter.copy(itemDocument));
				}
			}							
		}		
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		//Do not serialize any properties.
	}

	/**
	 * Caches classes to file.
	 * 
	 * @param operation: "read" or "write"
	 */	
	@SuppressWarnings("unchecked")
	public void caching(String operation) {
		final String cacheFile="classes";
		
		try {
			switch (operation) {	
				case "read" : {
					ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(cacheFile));
					classes = (Map<String, Integer>) ((ObjectInputStream) objectInputStream).readObject();
					objectInputStream.close();
				}
				case "write" : {
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(cacheFile));
					objectOutputStream.writeObject(classes);
					objectOutputStream.flush();
					objectOutputStream.close();
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	}	
	
	/**
	 * Closes the output. Should be called after the taxonomy serialization was finished.
	 *
	 * @throws IOException (if there was a problem closing the output)
	 */
	public void close() throws IOException {
		if(operation == Operation.EXTRACTTSV) {
			this.classesStream.close();
			this.subClassesStream.close();
		}
		else if (operation == Operation.EXTRACTJSON) {
			this.jsonStream.close();
		}
	}
	
}
