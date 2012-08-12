/**
 * 
 */
package com.spazzmania.archive;

import java.util.Map;

/**
 * @author tjbillingsley
 *
 */
public interface JobStepClass {
	public void setRunParameters(Map<String,String> runParms);
	public void execute();
}
