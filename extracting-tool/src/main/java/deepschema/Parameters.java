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

import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.json.jackson.JsonSerializer;
import org.wikidata.wdtk.rdf.RdfSerializer;

/**
 * Parameters for the extracting tool.
 *
 * @author Panayiotis Smeros
 *
 */
public class Parameters {

	/**************************************** Parameters ****************************************/

	/**
	 * Timeout to abort processing after a short while or 0 to disable timeout. If set, then the processing will cleanly exit after about this many seconds, as
	 * if the dump file would have ended there. This is useful for testing (and in particular better than just aborting the program) since it allows for final
	 * processing and proper closing to happen without having to wait for a whole dump file to process.
	 */
	static final int TIMEOUT_SEC = 0;

	/**
	 * If set to true, all example programs will run in offline mode. Only data dumps that have been downloaded in previous runs will be used.
	 */
	static final boolean OFFLINE_MODE = true;

	static final Output output = Output.TSV;

	static final Boolean useCache = false;

	static final Boolean filterCategories = false;

	static final Boolean filterBioDBs = true;

	static final Boolean includeInstances = false;

	/******************************************************************************************/

	static final String separator = "\t";

	static class WikidataClassProperties implements Serializable {

		private static final long serialVersionUID = 1L;
		String label;
		Set<Integer> subclassOf;

		public WikidataClassProperties() {
			this.subclassOf = new HashSet<Integer>();
		}
		public WikidataClassProperties(String label) {
			this.label = label;
			this.subclassOf = new HashSet<Integer>();
		}
	}

	static class WikidataInstanceProperties implements Serializable {

		private static final long serialVersionUID = 1L;
		String label;
		Set<Integer> instanceOf;

		public WikidataInstanceProperties() {
			this.instanceOf = new HashSet<Integer>();
		}
		public WikidataInstanceProperties(String label) {
			this.label = label;
			this.instanceOf = new HashSet<Integer>();
		}		
	}

	static Map<Integer, WikidataClassProperties> classes = new HashMap<>();
	static Map<Integer, WikidataInstanceProperties> instances = new HashMap<>();

	static OutputStream classesStream, subclassOfRelationsStream, instancesStream, instanceOfRelationsStream, jsonStream, txtStream, rdfStream;

	enum Operation {
		READ_CLASSES, ENHANCE_AND_FILTER, STRUCTURE_OUTPUT, EXPLORE_DATASET
	}

	static Operation operation;

	enum Output {
		JSON, RDF, TSV, TXT
	}

	static DatamodelConverter datamodelConverter;
	static JsonSerializer jsonSerializer;
	static RdfSerializer rdfSerializer;

}
