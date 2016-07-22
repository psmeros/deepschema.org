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
	 * Reads classes from dump. The enhancement/filtering of the classes/instances is pushed to this pass of the dump (wherever this is possible).
	 * 
	 * @param ItemDocument
	 */
	public static void readDump(ItemDocument itemDocument) {

		StatementGroup sg = null;

		// subclassOf(C1, C2) => Class(C1) /\ Class(C2) #RDFS
		if ((sg = itemDocument.findStatementGroup("P279")) != null && !filter(itemDocument)) {

			int wikidataClass = Integer.parseInt(sg.getSubject().getId().substring(1));

			// No need to check if the class already exists in the map because we read one entity at the time and there are no duplicates.
			classes.put(wikidataClass, new WikidataClassProperties(getLabel(itemDocument)));

			for (Statement s : sg.getStatements()) {
				ItemIdValue value = (ItemIdValue) s.getValue();
				if (value != null) {
					int wikidataSubClass = Integer.parseInt(value.getId().substring(1));

					if (!classes.containsKey(wikidataSubClass))
						classes.put(wikidataSubClass, new WikidataClassProperties());

					classes.get(wikidataClass).subclassOf.add(wikidataSubClass);
				}
			}
		}

		// instanceOf(I, C) => Class(C) #RDFS
		if ((sg = itemDocument.findStatementGroup("P31")) != null && !filter(itemDocument)) {

			int wikidataInstance = Integer.parseInt(sg.getSubject().getId().substring(1));

			// No need to check if the instance already exists in the map because we read one entity at the time and there are no duplicates.
			if (includeInstances)
				instances.put(wikidataInstance, new WikidataInstanceProperties(getLabel(itemDocument)));

			for (Statement s : sg.getStatements()) {
				ItemIdValue value = (ItemIdValue) s.getValue();
				if (value != null) {
					int wikidataClass = Integer.parseInt(value.getId().substring(1));

					if (!classes.containsKey(wikidataClass))
						classes.put(wikidataClass, new WikidataClassProperties());

					if (includeInstances)
						instances.get(wikidataInstance).instanceOf.add(wikidataClass);
				}
			}
		}
	}

	/**
	 * Finds the English label of an entity.
	 * 
	 * @param ItemDocument
	 * @return label
	 */
	public static String getLabel(ItemDocument itemDocument) {
		String label = itemDocument.findLabel("en");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("uk");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-us");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-gb");
		if (label == null || label.trim() == "")
			label = itemDocument.findLabel("en-ca");

		if (label == null || label.trim() == "")
			label = null;
		else
			label = label.replace(separator, " ");

		return label;
	}

	/**
	 * Finds the labels of the entities and applies the filters.
	 * 
	 * @param ItemDocument
	 */
	public static Boolean filter(ItemDocument itemDocument) {

		String label = getLabel(itemDocument);

		// Filter classes with no English label.
		if (label == null)
			return true;

		// Filter category classes.
		if (filterCategories && ((label.startsWith("Cat") || label.startsWith("Кат")) && label.contains(":")))
			return true;

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
										return true;
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
										return true;
									}
								}
							}
						}
					}
				}
			}
		}
		return false;
	}

	/**
	 * Finds the labels of the classes and applies the filters. This second pass is only for the classes that have not been enhanced/filter yet.
	 * 
	 * @param ItemDocument
	 */
	public static void enhanceAndFilter(ItemDocument itemDocument) {
		int wikidataClass = Integer.parseInt(itemDocument.getEntityId().getId().substring(1));

		if (!classes.containsKey(wikidataClass) || classes.get(wikidataClass).label != null)
			return;

		if (filter(itemDocument))
			classes.remove(wikidataClass);
		else
			classes.get(wikidataClass).label = getLabel(itemDocument);
	}

	/**
	 * Structures the output to JSON or RDF format.
	 * 
	 * @param ItemDocument
	 */
	public static void structureOutput(ItemDocument itemDocument) {
		if (classes.containsKey(Integer.parseInt(itemDocument.getEntityId().getId().substring(1))))
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
			if (classes.containsKey(Integer.parseInt(itemDocument.getEntityId().getId().substring(1)))) {

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

					if (!foundProvenance)
						txtStream.write(("other \n").getBytes());
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else if (operation.equals("inspectLanguages1")) {
			if (classes.containsKey(Integer.parseInt(itemDocument.getEntityId().getId().substring(1)))) {

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
			int wikidataClass = Integer.parseInt(itemDocument.getEntityId().getId().substring(1));

			if (classes.containsKey(wikidataClass)) {

				try {
					txtStream.write((classes.get(wikidataClass).label + "\t" + itemDocument.getLabels().size() + "\n").getBytes());
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
