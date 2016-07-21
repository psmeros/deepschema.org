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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.Reference;
import org.wikidata.wdtk.datamodel.interfaces.Snak;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.dumpfiles.DumpContentType;
import org.wikidata.wdtk.dumpfiles.DumpProcessingController;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor.TimeoutException;

import static deepschema.Parameters.*;

/**
 * Wikidata dump operations.
 *
 * @author Panayiotis Smeros
 *
 */
public class DumpOperations {

	/**
	 * Reads from Dump and applies RDFS rules for extraction.
	 * 
	 * @param ItemDocument
	 */
	public static void readFromDump(ItemDocument itemDocument) {

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
						if (!instances.containsKey(sg.getSubject().getId()))
							instances.put(sg.getSubject().getId(), new WikidataInstanceProperties());

						instances.get(sg.getSubject().getId()).instanceOf.add(value.getId());
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
	public static void enhanceAndFilter(ItemDocument itemDocument) {
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
			instances.get(currentId).label = label;

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
	public static void structureOutput(ItemDocument itemDocument) {
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
	public static void exploreDataset(ItemDocument itemDocument) {

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
