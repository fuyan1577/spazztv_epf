/**
 * 
 */
package com.spazztv.epf.dao;

import java.lang.reflect.Constructor;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFFileReader;

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
	
	public static EPFFileReaderFactory getInstance() {
		if (factory == null) {
			factory = new EPFFileReaderFactory();
		}
		return factory;
	}

	public static EPFFileReaderFactory getInstance(EPFConfig epfConfig) {
		factory = getInstance();
		factory.setEpfConfig(epfConfig);
		return factory;
	}
	
	public EPFFileReader getFileReader(String epfFileName) {
		
		return null;
	}
	
	private EPFFileReader newFileReaderInstance(String epfFileName)
			throws EPFDbException {
		EPFFileReader fileReader = null;

		try {
			Class<EPFFileReader> fileReaderClass = Class.forName(epfConfig.getEpfFileReaderClass());
			Constructor<EPFFileReader> constructor = fileReaderClass.getConstructor(String.class,String.class,String.class);
			fileReader = (EPFFileReader)constructor.newInstance(epfFileName,epfConfig.getFieldSeparator(),epfConfig.getRecordSeparator());
		} catch (Exception e) {
			throw new EPFDbException(e.getMessage());
		}
		
		return fileReader;
	}
	
	
	/**
	 * @return the epfConfig
	 */
	public EPFConfig getEpfConfig() {
		return epfConfig;
	}

	/**
	 * @param epfConfig the epfConfig to set
	 */
	public void setEpfConfig(EPFConfig epfConfig) {
		this.epfConfig = epfConfig;
	}


}
