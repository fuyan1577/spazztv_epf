/**
 * 
 */
package com.spazztv.epf.dao;

import java.lang.reflect.Constructor;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFImporterException;

/**
 * A Singleton Factory Object which creates instances of classes which implement
 * the EPFFileReader interface. The classes used are those designated in the
 * EPFConfig object.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFFileReaderFactory {
	private static EPFFileReaderFactory factory;
	private EPFConfig epfConfig;

	private EPFFileReaderFactory() {
	}
	
	public static EPFFileReader getFileReader(EPFConfig epfConfig, String epfFileName) throws EPFImporterException {
		if (factory == null) {
			factory = new EPFFileReaderFactory();
		}
		factory.setEpfConfig(epfConfig);
		return factory.newFileReader(epfFileName);
	}
	
	@SuppressWarnings("unchecked")
	private EPFFileReader newFileReader(String epfFileName) throws EPFImporterException {
		EPFFileReader fileReader = null;

		try {
			Class<EPFFileReader> fileReaderClass = (Class<EPFFileReader>)Class.forName(epfConfig.getEpfFileReaderClass());
			Constructor<EPFFileReader> constructor = fileReaderClass.getConstructor(String.class,String.class,String.class);
			fileReader = (EPFFileReader)constructor.newInstance(epfFileName,epfConfig.getFieldSeparator(),epfConfig.getRecordSeparator());
		} catch (Exception e) {
			throw new EPFImporterException(e.getMessage());
		}
		
		return fileReader;
	}
	
	
	/**
	 * @param epfConfig the epfConfig to set
	 */
	private void setEpfConfig(EPFConfig epfConfig) {
		this.epfConfig = epfConfig;
	}


}
