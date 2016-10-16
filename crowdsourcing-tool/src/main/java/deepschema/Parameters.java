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
import java.util.ArrayList;
import java.util.List;

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
	static final int TIMEOUT_SEC = 20;

	/**
	 * If set to true, all example programs will run in offline mode. Only data dumps that have been downloaded in previous runs will be used.
	 */
	static final boolean OFFLINE_MODE = true;

	static final Boolean useCache = false;
	
	static final String gluingFile = "/Users/smeros/Downloads/gluing_thr_0.9.nt";
	static final String schemaFile = "/Users/smeros/Downloads/schema.nt";


	/******************************************************************************************/

	static final String separator = "\t";

	
	static class CrowdsourcingInfo implements Serializable {

		private static final long serialVersionUID = 1L;
		String WikidataLabel;
		String WikidataURL;
		String WikidataDescription;
		String SchemaLabel;
		String SchemaURL;
		String SchemaDescription;
		String relation;
	}	
	
	static List<CrowdsourcingInfo> crowdsourcingInfoList = new ArrayList<>();
	static OutputStream outputStream;

}
