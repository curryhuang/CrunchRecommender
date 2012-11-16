package cn.edu.bjtu.cit.cli;

import java.util.Map;

import com.google.common.collect.Maps;

public class OptionParser {
	
	private Map<String, Option> options;
	
	public OptionParser(String[] args){
		options = Maps.newHashMap();
		parseCommandLine(args);
	}
	
	public boolean hasOption(String option){
		return options.containsKey(option);
	}
	
	public Option getOption(String option){
		if(hasOption(option)){
			return options.get(option);
		}
		throw new IllegalArgumentException("Unkonwn option name " + option);
	}
	
	private void parseCommandLine(String[] args){
		for (String arg : args) {
			if (arg.contains("=")) {
				String[] tokens = arg.split("=");
				String name = tokens[0].toLowerCase();
				options.put(name, new Option(name, tokens[1]));
			}
		}
	}
}
