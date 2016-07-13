package deepschema;

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
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.openrdf.rio.RDFFormat;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.Reference;
import org.wikidata.wdtk.datamodel.interfaces.Snak;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.json.jackson.JacksonObjectFactory;
import org.wikidata.wdtk.datamodel.json.jackson.JsonSerializer;
import org.wikidata.wdtk.dumpfiles.DumpProcessingController;
import org.wikidata.wdtk.rdf.PropertyRegister;
import org.wikidata.wdtk.rdf.RdfSerializer;

/**
 * deepchema.org extracting tool.
 *
 * @author Panayiotis Smeros
 *
 */
public class ExtractingTool implements EntityDocumentProcessor {

	/********** Parameters **********/
	Output output = Output.TXT;

	Boolean useCache = true;

	Boolean filterCategories = false;

	Boolean filterBioDBs = true;

	Boolean includeInstances = false;
	/******************************/

	OutputStream classesStream, subclassOfRelationsStream, instancesStream, instanceOfRelationsStream, jsonStream, txtStream, rdfStream;

	enum Operation {
		READ_FROM_DUMP, ENHANCE_AND_FILTER, STRUCTURE_OUTPUT, EXPLORE_DATASET
	}

	Operation operation;

	enum Output {
		JSON, RDF, TSV, TXT
	}

	final String separator = "\t";

	static class WikidataClassProperties implements Serializable {

		private static final long serialVersionUID = 1L;
		String label;
		Set<String> subclassOf;
		Set<String> instances;

		public WikidataClassProperties() {
			this.label = "";
			this.subclassOf = new HashSet<String>();
			this.instances = new HashSet<String>();
		}
	}

	Map<String, WikidataClassProperties> classes;
	Map<String, String> instances;

	DatamodelConverter datamodelConverter;
	JsonSerializer jsonSerializer;
	RdfSerializer rdfSerializer;

	/**
	 * Constructor. Opens the file that we want to write to.
	 *
	 * @throws IOException (if there is a problem opening the output file(s))
	 */
	public ExtractingTool() throws IOException {

		this.classes = new HashMap<>();
		this.instances = new HashMap<>();

		if (output == Output.TSV) {
			this.classesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("classes.tsv.gz")));
			this.subclassOfRelationsStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("subclassOfRelations.tsv.gz")));
			if (includeInstances) {
				this.instancesStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("instances.tsv.gz")));
				this.instanceOfRelationsStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("instanceOfRelations.tsv.gz")));
			}
		}
		else if (output == Output.JSON) {
			this.jsonStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("classesAndInstances.json.gz")));

			// DataModel
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
		else if (output == Output.RDF) {
			this.rdfStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("classesAndInstances.nt.gz")));

			// DataModel
			this.datamodelConverter = new DatamodelConverter(new JacksonObjectFactory());
			// Do not copy references.
			this.datamodelConverter.setOptionDeepCopyReferences(false);
			// Only copy English labels, descriptions, and aliases.
			this.datamodelConverter.setOptionLanguageFilter(Collections.singleton("en"));
			// Copy statements of all the properties.
			this.datamodelConverter.setOptionPropertyFilter(null);
			// Do not copy sitelinks.
			this.datamodelConverter.setOptionSiteLinkFilter(Collections.<String> emptySet());

			this.rdfSerializer = new RdfSerializer(RDFFormat.NTRIPLES, rdfStream, new DumpProcessingController("wikidatawiki").getSitesInformation(), PropertyRegister.getWikidataPropertyRegister());
			// Serialize simple statements (and nothing else) for all items
			this.rdfSerializer.setTasks(RdfSerializer.TASK_ITEMS | RdfSerializer.TASK_SIMPLE_STATEMENTS);
			this.rdfSerializer.open();
		}
		else if (output == Output.TXT) {
			this.txtStream = new GzipCompressorOutputStream(new BufferedOutputStream(ExampleHelpers.openExampleFileOuputStream("output.txt.gz")));
		}
	}

	/**
	 * Runs the example program.
	 *
	 * @param args
	 * @throws IOException (if there was a problem in writing the output file(s))
	 */
	public static void main(String[] args) throws IOException {
		ExampleHelpers.configureLogging();

		new ExtractingTool().init();
	}

	/**
	 * Initializes the procedure.
	 */
	public void init() {
		if (!useCache) {
			operation = Operation.READ_FROM_DUMP;
			ExampleHelpers.processEntitiesFromWikidataDump(this);
			operation = Operation.ENHANCE_AND_FILTER;
			ExampleHelpers.processEntitiesFromWikidataDump(this);
			writeOutput();
			cache("write");
		}
		else {
			cache("read");
			writeOutput();
		}
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
					for (Entry<String, String> entry : instances.entrySet())
						instancesStream.write((entry.getKey().substring(1) + separator + entry.getValue() + "\n").getBytes());

					for (Entry<String, WikidataClassProperties> entry : classes.entrySet())
						for (String value : entry.getValue().instances)
							instanceOfRelationsStream.write((value.substring(1) + separator + entry.getKey().substring(1) + "\n").getBytes());
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
				ExampleHelpers.processEntitiesFromWikidataDump(this);
				jsonStream.close();
			}
			else if (output == Output.RDF) {
				operation = Operation.STRUCTURE_OUTPUT;
				ExampleHelpers.processEntitiesFromWikidataDump(this);
				rdfStream.close();
			}
			else if (output == Output.TXT) {
				operation = Operation.EXPLORE_DATASET;
				ExampleHelpers.processEntitiesFromWikidataDump(this);
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
					instances = (Map<String, String>) ((ObjectInputStream) objectInputStream).readObject();
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

	/**
	 * Reads from Dump and applies RDFS rules for extraction.
	 * 
	 * @param ItemDocument
	 */
	void readFromDump(ItemDocument itemDocument) {

		StatementGroup sg = null;

		// instanceOf(I, C) => Class(C) #RDFS
		sg = itemDocument.findStatementGroup("P31");

		if (sg != null) {
			for (Statement s : sg.getStatements()) {
				ItemIdValue value = (ItemIdValue) s.getValue();
				if (value != null) {
					if (!classes.containsKey(value.getId()))
						classes.put(value.getId(), new WikidataClassProperties());

					if (includeInstances) {
						classes.get(value.getId()).instances.add(sg.getSubject().getId());

						if (!instances.containsKey(sg.getSubject().getId()))
							instances.put(sg.getSubject().getId(), "");
					}
				}
			}
		}

		// subclassOf(C1, C2) => Class(C1) /\ Class(C2) #RDFS
		sg = itemDocument.findStatementGroup("P279");

		if (sg != null) {
			if (!classes.containsKey(sg.getSubject().getId()))
				classes.put(sg.getSubject().getId(), new WikidataClassProperties());

			for (Statement s : sg.getStatements()) {
				ItemIdValue value = (ItemIdValue) s.getValue();
				if (value != null) {
					if (!classes.containsKey(value.getId()))
						classes.put(value.getId(), new WikidataClassProperties());

					classes.get(sg.getSubject().getId()).subclassOf.add(value.getId());
				}
			}
		}
	}

	/**
	 * Finds the labels of the classes and applies the filters.
	 * 
	 * @param ItemDocument
	 */
	void enhanceAndFilter(ItemDocument itemDocument) {
		Boolean isClass = false, isInstance = false;
		String currentId = itemDocument.getEntityId().getId();

		if (classes.containsKey(currentId))
			isClass = true;
		else if (instances.containsKey(currentId))
			isInstance = true;
		else
			return;

		// Add english label.
		String label = itemDocument.findLabel("en");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("uk");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-us");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-gb");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-ca");

		if (label == null || label.trim() == "") {
			if (isClass)
				classes.remove(currentId);
			else if (isInstance)
				instances.remove(currentId);
			return;
		}
		label = label.replace(separator, " ");

		if (isClass)
			classes.get(currentId).label = label;
		else if (isInstance)
			instances.put(currentId, label);

		if (isClass) {
			// Filter category classes.
			if (filterCategories) {
				if ((label.startsWith("Cat") || label.startsWith("Кат")) && label.contains(":")) {
					classes.remove(currentId);
					return;
				}
			}

			// Filter classes from Biological DBs.
			if (filterBioDBs) {
				for (StatementGroup sg : itemDocument.getStatementGroups()) {
					for (Statement s : sg) {
						for (Iterator<? extends Reference> it = s.getReferences().iterator(); it.hasNext();) {
							for (Iterator<Snak> sn = it.next().getAllSnaks(); sn.hasNext();) {
								Snak snak = sn.next();
								if (snak.getValue() != null) {
									String pid = snak.getPropertyId().getId();
									String vid = snak.getValue().toString();

									if (pid.equals("P143")) {
										if (vid.contains("Q20641742") || // NCBI Gene
										vid.contains("Q905695") || // UniProt
										vid.contains("Q1344256") || // Ensembl
										vid.contains("Q22230760") || // Ontology Lookup Service
										vid.contains("Q1345229") || // Entrez
										vid.contains("Q468215") || // HomoloGene
										vid.contains("Q13651104") || vid.contains("Q15221937") || vid.contains("Q18000294") || vid.contains("Q1936589") || vid.contains("Q19315626")) { // minerals
											classes.remove(currentId);
											return;
										}
									}

									if (pid.equals("P248")) {
										if (vid.contains("Q20950174") || // NCBI homo sapiens annotation release 107
										vid.contains("Q20973051") || // NCBI mus musculus annotation release 105
										vid.contains("Q2629752") || // Swiss-Prot
										vid.contains("Q905695") || // UniProt
										vid.contains("Q20641742") || // NCBI Gene
										vid.contains("Q21996330") || // Ensembl Release 83
										vid.contains("Q135085") || // Gene Ontology
										vid.contains("Q5282129") || // Disease Ontology
										vid.contains("Q20976936") || // HomoloGene build68
										vid.contains("Q17939676") || // NCBI Homo sapiens Annotation Release 106
										vid.contains("Q21234191") || // NuDat
										vid.contains("Q13651104") || vid.contains("Q15221937") || vid.contains("Q18000294") || vid.contains("Q1936589") || vid.contains("Q19315626")) { // minerals
											classes.remove(currentId);
											return;
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Structures the output to JSON or RDF format.
	 * 
	 * @param ItemDocument
	 */
	public void structureOutput(ItemDocument itemDocument) {
		if (classes.containsKey(itemDocument.getEntityId().getId()))
			if (output == Output.JSON)
				jsonSerializer.processItemDocument(datamodelConverter.copy(itemDocument));
			else if (output == Output.RDF)
				rdfSerializer.processItemDocument(datamodelConverter.copy(itemDocument));
	}

	/**
	 * Explores Dataset.
	 * 
	 * @param ItemDocument
	 */
	public void exploreDataset(ItemDocument itemDocument) {

		final String operation = "inspectProvenance";

		if (operation.equals("inspectProvenance")) {
			if (classes.containsKey(itemDocument.getEntityId().getId())) {

				try {
					Boolean foundProvenance = false;
					// Freebase ID
					if (itemDocument.hasStatement("P646")) {
						txtStream.write(("freebase \n").getBytes());
						foundProvenance = true;
					}

					// GND ID
					if (itemDocument.hasStatement("P227")) {
						txtStream.write(("GND \n").getBytes());
						foundProvenance = true;
					}

					StatementGroup sg = null;

					// Equivalent class
					sg = itemDocument.findStatementGroup("P1709");

					if (sg != null) {
						for (Statement s : sg.getStatements()) {
							Value value = s.getValue();
							if (value != null) {
								if (value.toString().contains("dbpedia")) {
									txtStream.write(("dbpedia \n").getBytes());
									foundProvenance = true;
								}
								else if (value.toString().contains("schema.org")) {
									txtStream.write(("schema.org \n").getBytes());
									foundProvenance = true;
								}
							}
						}
					}

					Set<String> prov = new HashSet<String>();
					for (StatementGroup stg : itemDocument.getStatementGroups()) {
						for (Statement s : stg) {
							for (Iterator<? extends Reference> it = s.getReferences().iterator(); it.hasNext();) {
								for (Iterator<Snak> sn = it.next().getAllSnaks(); sn.hasNext();) {
									Snak snak = sn.next();
									// "imported from" and "stated in"
									if (snak.getPropertyId().getId().equals("P143") || snak.getPropertyId().getId().equals("P248")) {
										prov.add(snak.getValue().toString());
									}
								}
							}
						}
					}
					for (Iterator<String> s = prov.iterator(); s.hasNext();) {
						try {
							txtStream.write((s.next() + "\n").getBytes());
							foundProvenance = true;
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					}

					if (!foundProvenance) {
						txtStream.write(("other \n").getBytes());
						System.out.println(itemDocument.getEntityId());
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else if (operation.equals("inspectLanguages1")) {
			if (classes.containsKey(itemDocument.getEntityId().getId())) {

				for (Iterator<String> it = itemDocument.getLabels().keySet().iterator(); it.hasNext();) {
					try {
						txtStream.write((it.next() + "\n").getBytes());
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		else if (operation.equals("inspectLanguages2")) {
			if (classes.containsKey(itemDocument.getEntityId().getId())) {

				try {
					txtStream.write((classes.get(itemDocument.getEntityId().getId()).label + "\t" + itemDocument.getLabels().size() + "\n").getBytes());
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		// Do not serialize any properties.
	}
}
