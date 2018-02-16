package com.tony.kafka.domain;

public class EventMessage {

	private String name;

	public EventMessage() {}

	public EventMessage(String name) {
		super();
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "EventMessage [name=" + name + "]";
	}
	
}
