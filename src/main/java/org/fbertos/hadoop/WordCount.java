package org.fbertos.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class WordCount {
	@SuppressWarnings("deprecation")
	public static void main(String [] args) throws Exception {
		Configuration c = new Configuration();

		
		String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
		//Path input = new Path(files[0]);
		
		//Path output = new Path(files[1]);
		Job j = new Job(c,"wordcount");
		
		
		MultipleInputs.addInputPath(j, new Path("/input/employee.xml"), EmployeeXmlInputFormat.class);
		MultipleInputs.addInputPath(j, new Path("/input/department.xml"), DepartmentXmlInputFormat.class);
		
		j.setJarByClass(WordCount.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, new Path("/output"));
		System.exit(j.waitForCompletion(true)?0:1);
	}
	

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			
			
			try {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				StringBuilder xmlStringBuilder = new StringBuilder();
				xmlStringBuilder.append(line);
				ByteArrayInputStream input = new ByteArrayInputStream(xmlStringBuilder.toString().getBytes());
				Document doc = builder.parse(input);
				
				Element root = doc.getDocumentElement();
				String dep = "";
				String id = root.getAttribute("id");
				String name = root.getTextContent();
				String type = "employee";
				String newkey = id;
				
				if ("employee".equals(root.getNodeName())) {				
					dep = root.getAttribute("department");
					newkey = dep;
				}
				else {
					type = "department";
				}
				
				String newline = type + ";" + id + ";" + name + ";" + dep;
				
				Text outputKey = new Text(newkey);
				Text outputValue = new Text(newline);

				con.write(outputKey, outputValue);	
				
			} catch (Exception e) {
				e.printStackTrace();
			}			
			
			/*
			String[] words = line.split(";");
			
			FileSplit fileSplit = (FileSplit)con.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String department = "";
			
			if ("department.csv".equals(filename))
				department = words[0];
			else
				department = words[2];
			
			String newline = filename + ";" + line;
			
			Text outputKey = new Text(department);
			Text outputValue = new Text(newline);
			
			
			con.write(outputKey, outputValue);
			*/
			//String[] words = line.split(";");
			
			/*
			for (String word: words) {
				Text outputKey = new Text(word.toUpperCase().trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
			*/
		}
	}
	
	public static class ReduceForWordCount extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException {
			/*
			Text newkey = new Text(word);

			for (Text value : values) {
				con.write(newkey, new Text(value));
			}
			*/
			
			String department = "";
			ArrayList<String> list = new ArrayList<String>();
			
			for (Text value : values) {
				String[] line = value.toString().split(";");
				
				if ("department".equals(line[0]))
					department = line[2];
				else
					list.add(value.toString());
			}
			
			for (int i=0; i<list.size(); i++) {
				String line = list.get(i);
				String[] fields = line.split(";");
				String newline = fields[1] + ";" + fields[2] + ";" + department;
				Text newkey = new Text("");
				
				con.write(newkey, new Text(newline));
			}
		}
	}
}
