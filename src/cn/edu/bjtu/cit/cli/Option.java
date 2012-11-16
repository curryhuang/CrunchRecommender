package cn.edu.bjtu.cit.cli;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class Option {
	private String name;
	private String value;
	
	public Option(){
		
	}
	
	public Option(String name, String value){
		this.name = name;
		this.value = value;
	}
	
	public Option setName(String name){
		this.name = name;
		return this;
	}
	
	public Option setValue(String value){
		this.value = value;
		return this;
	}
	
	public String getName(){
		return this.name;
	}
	
	public String getValue(){
		return this.value;
	}
	
	@Override
	public boolean equals(Object other){
		if(!(other instanceof Option)){
			return false;
		}
		Option obj = (Option)other;
		return name.equals(obj.getName()) && value.equals(obj.getValue());
	}
	
	@Override
	public int hashCode(){
		return new HashCodeBuilder().append(name).append(value).toHashCode();
	}
	
	@Override
	public String toString(){
		return "option " + name + ": " + value;
	}
}
