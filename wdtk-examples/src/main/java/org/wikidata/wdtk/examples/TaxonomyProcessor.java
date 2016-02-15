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
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
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
public class TaxonomyProcessor implements EntityDocumentProcessor {

	final OutputStream classesStream;
	final OutputStream jsonStream;
	
	public enum Operation {FINDCLASSES, EXTRACTJSON, EXTRACTCLASSES}
	public static Operation operation;
	
	Set <String> classes;

	final DatamodelConverter datamodelConverter;
	final JsonSerializer jsonSerializer;


	/**
	 * Runs the example program.
	 *
	 * @param args
	 * @throws IOException (if there was a problem in writing the output file)
	 */
	public static void main(String[] args) throws IOException {
		ExampleHelpers.configureLogging();

		TaxonomyProcessor jsonTaxonomyProcessor = new TaxonomyProcessor();
		operation = Operation.FINDCLASSES;
		ExampleHelpers.processEntitiesFromWikidataDump(jsonTaxonomyProcessor);
		operation = Operation.EXTRACTJSON;
		ExampleHelpers.processEntitiesFromWikidataDump(jsonTaxonomyProcessor);
		
		jsonTaxonomyProcessor.close();
	}

	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 * @throws IOException (if there is a problem opening the output file)
	 */
	public TaxonomyProcessor() throws IOException {

		// The (compressed) file we write to.
		this.classesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedClasses.gz")));
		this.jsonStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("extractedClasses.json.gz")));
		this.classes = new HashSet<>();

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

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		if (operation == Operation.FINDCLASSES)
		{
			for (StatementGroup sg : itemDocument.getStatementGroups()) {
				
				//subclassOf(C1, C2) => Class(C1) /\ Class(C2) #RDFS
				if ("P279".equals(sg.getProperty().getId())) {
					this.classes.add(sg.getSubject().getId());
					
					for (Statement s : sg.getStatements()) {
						ItemIdValue value = (ItemIdValue) s.getValue();
						if (value != null)
							this.classes.add(value.getId());					 
					}
				}

				//instanceOf(I, C) => Class(C) #RDFS
				if ("P31".equals(sg.getProperty().getId())) {
					for (Statement s : sg.getStatements()) {			
						ItemIdValue value = (ItemIdValue) s.getValue();
						if (value != null)
							this.classes.add(value.getId());
					}
				}
			}			
		}
		else {
			if(classes.contains(itemDocument.getEntityId().getId())) {				
				for (Entry <String, MonolingualTextValue> label : itemDocument.getLabels().entrySet()) {
					if (label.getKey().contains("en") || label.getKey().equals("gb") || label.getKey().equals("us")) {
						if(!label.getValue().getText().toLowerCase().startsWith("category")) {
							if (operation == Operation.EXTRACTCLASSES){
								try {
									this.classesStream.write((itemDocument.getEntityId()+"\n").getBytes());
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
				
			}
			
		}		
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		// we do not serialize any properties
	}

	
	/**
	 * Closes the output. Should be called after the taxonomy serialization was
	 * finished.
	 *
	 * @throws IOException (if there was a problem closing the output)
	 */
	public void close() throws IOException {
		this.classesStream.close();
		this.jsonStream.close();
	}

}
