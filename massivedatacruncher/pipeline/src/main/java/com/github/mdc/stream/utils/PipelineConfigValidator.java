package com.github.mdc.stream.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;

public class PipelineConfigValidator implements ConfigValidator{

	public static final String BLOCKSIZENUMBER = "The block size should be number";
	public static final String BLOCKSIZERANGE = "The block size should greater than 1 and less than 128";
	public static final String BATCHSIZENUMBER = "The batch size should be number";
	public static final String MESOSTRUEFALSE = "Mesos mode config must be set to true or false";
	public static final String YARNTRUEFALSE = "Yarn mode config must be set to true or false";
	public static final String LOCALTRUEFALSE = "Local mode config must be set to true or false";
	public static final String JGROUPSTRUEFALSE = "JGroups mode config must be set to true or false";
	public static final String ERRORJGROUPSYARNLOCALMESOSSET = "Only one mode needs to set for session either JGROUPS or YARN or LOCAL or MESOS";
	public static final String MINMEMNUMBER = "The minimum heap size should be number";
	public static final String MAXMEMNUMBER = "The maximum heap size should be number";
	public static final String GCTYPEERROR = "The GC Type should be either G1GC (-XX:+UseG1GC) or ZGC (-XX:+UseZGC)";
	public static final String NUMCONTERROR = "The numnber of containers should be positive number";
	public static final String MESOSMASTERNOTSETERROR = "The mesos master url ([mesoshost]:[mesosport]) needs to set";
	@Override
	public List<String> validate(PipelineConfig pc) {
		var errormessage = new ArrayList<String>();
		var value = pc.getBlocksize();
		if(!StringUtils.isNumeric(value)) {
			errormessage.add(BLOCKSIZENUMBER);
		} else {
			long i = Long.parseLong(value);
			if(i<1 || i>256) {
				errormessage.add(BLOCKSIZERANGE);
			}
		}
		value = pc.getBatchsize();
		if(!StringUtils.isNumeric(value)) {
			errormessage.add(BATCHSIZENUMBER);
		}
		var mesos = pc.getMesos();
		var yarn = pc.getYarn();
		var local = pc.getLocal();
		var jgroups = pc.getJgroups();
		var ismodetruefalseerror = false;
		if(!(mesos.equalsIgnoreCase("true")||mesos.equalsIgnoreCase("false"))) {
			ismodetruefalseerror = true;
			errormessage.add(MESOSTRUEFALSE);
		}
		if(!(yarn.equalsIgnoreCase("true")||yarn.equalsIgnoreCase("false"))) {
			ismodetruefalseerror = true;
			errormessage.add(YARNTRUEFALSE);
		}
		if(!(local.equalsIgnoreCase("true")||local.equalsIgnoreCase("false"))) {
			ismodetruefalseerror = true;
			errormessage.add(LOCALTRUEFALSE);
		}
		if(!(jgroups.equalsIgnoreCase("true")||jgroups.equalsIgnoreCase("false"))) {
			ismodetruefalseerror = true;
			errormessage.add(JGROUPSTRUEFALSE);
		}
		if(!(!ismodetruefalseerror&&(
				(yarn.equalsIgnoreCase("true")&&mesos.equalsIgnoreCase("false")
						&&local.equalsIgnoreCase("false")&&jgroups.equalsIgnoreCase("false"))
				||(yarn.equalsIgnoreCase("false")&&mesos.equalsIgnoreCase("true")
						&&local.equalsIgnoreCase("false")&&jgroups.equalsIgnoreCase("false"))
				||(yarn.equalsIgnoreCase("false")&&mesos.equalsIgnoreCase("false")
						&&local.equalsIgnoreCase("true")&&jgroups.equalsIgnoreCase("false"))
				||(yarn.equalsIgnoreCase("false")&&mesos.equalsIgnoreCase("false")
						&&local.equalsIgnoreCase("false")&&jgroups.equalsIgnoreCase("true"))
				||(yarn.equalsIgnoreCase("false")&&mesos.equalsIgnoreCase("false")
						&&local.equalsIgnoreCase("false")&&jgroups.equalsIgnoreCase("false"))
				))) {
			errormessage.add(ERRORJGROUPSYARNLOCALMESOSSET);
		}
		value = pc.getMinmem();
		if(!StringUtils.isNumeric(value)) {
			errormessage.add(MINMEMNUMBER);
		}
		value = pc.getMaxmem();
		if(!StringUtils.isNumeric(value)) {
			errormessage.add(MAXMEMNUMBER);
		}
		value = pc.getGctype();
		if(!(value.equals(MDCConstants.ZGC)||value.equals(MDCConstants.G1GC))) {
			errormessage.add(GCTYPEERROR);
		}
		value = pc.getNumberofcontainers();
		if(!StringUtils.isNumeric(value)) {
			errormessage.add(NUMCONTERROR);
		}
		value = pc.getMesosmaster();
		if(!ismodetruefalseerror&&mesos.equalsIgnoreCase("true")&&value.trim().equals("")) {
			errormessage.add(MESOSMASTERNOTSETERROR);
		}
		return errormessage;
	}

}
