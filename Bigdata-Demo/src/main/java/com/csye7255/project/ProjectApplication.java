package com.csye7255.project;

import javax.servlet.Filter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.filter.ShallowEtagHeaderFilter;

@SpringBootApplication
public class ProjectApplication {
	
	@Bean
	public Filter shallowEtagFilter() {
		return new ShallowEtagHeaderFilter();
	}

	public static void main(String[] args) {

		SpringApplication.run(ProjectApplication.class, args);
	}

}

