package org.wso2.siddhi.extension;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.core.util.parser.ExecutorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;
import org.xml.sax.InputSource;

@SiddhiExtension(namespace = "mlearn", function = "getModelPrediction")
public class PmmlModelExecutor extends TransformProcessor {

	private Map<String, Integer> parameterPositions = new HashMap<String, Integer>();
	Map<FieldName, FieldValue> inData = new HashMap<FieldName, FieldValue>();
	private List<FieldName> allFields;
	private List<FieldName> predictedFields;
	private List<FieldName> outputFields;
	private Evaluator evaluator;
	private List<FieldName> inputs = new ArrayList<FieldName>();
	private Map<FieldName, ?> result;
	private String pmml = null;
	private PMML model = null;
	private FieldName featureName;
	private Object featureValue;

	public PmmlModelExecutor() {
	}

	@Override
	protected InStream processEvent(InEvent inEvent) {
		/*
		 * read the value of each input attribute from input stream
		 *   and populate a single row
		 */
		for (FieldName inputfield : inputs) {
			featureName = new FieldName(inputfield.getValue());
			featureValue = inEvent.getData(parameterPositions.get(inputfield.toString()));
			inData.put(featureName, EvaluatorUtil.prepare(evaluator, featureName, featureValue));
		}
		/*
		 * evaluate the model using the above values
		 * and get the model's output
		 */
		result = evaluator.evaluate(inData);
		Object[] resltObjct = new Object[result.size()];
		int i = 0;
		for (FieldName fieldName : result.keySet()) {
			resltObjct[i++] = EvaluatorUtil.decode(result.get(fieldName));
		}
		return new InEvent("pmmlPredictedStream", System.currentTimeMillis(), resltObjct);
	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();
		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {
				transformedListEvent.addEvent((Event) processEvent((InEvent) event));
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		return new Object[] { parameterPositions };
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void restoreState(Object[] objects) {
		if (objects.length > 0 && objects[0] instanceof Map) {
			parameterPositions = (Map<String, Integer>) objects[0];
		}
	}

	@Override
	protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {
		for (Expression parameter : parameters) {
			if (parameter instanceof Variable) {
				Variable var = (Variable) parameter;
				String attributeName = var.getAttributeName();
				parameterPositions.put(attributeName, inStreamDefinition.getAttributePosition(attributeName));
			}
		}
		try {
			if (parameters[0] instanceof StringConstant) {
				Expression expression = parameters[0];
				ExpressionExecutor pmmlExecutor = ExecutorParser.parseExpression(expression, null,elementId, false,siddhiContext);
				pmml = (String) pmmlExecutor.execute(null);
			} else {
				Exception exception = new Exception("Cannot find a pmml definition. \nPlease provide the pmml file path" +
						" or the full pmml definition as the first attribute in the query");
				throw exception;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// if the pmml file path is given
		if (isFilePath(pmml.toString())) {
			String path = pmml.toString();
			File pmmlFile = new File(path);
			try {
				model = IOUtil.unmarshal(pmmlFile);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// else, if the full pmml definition is given
		else {
			InputSource pmmlSource = new InputSource(new StringReader(pmml.toString()));
			try {
				model = IOUtil.unmarshal(pmmlSource);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		PMMLManager pmmlManager = new PMMLManager(model);
		evaluator =(Evaluator) pmmlManager.getModelManager(null,ModelEvaluatorFactory.getInstance());
		allFields = evaluator.getActiveFields();
		predictedFields = evaluator.getPredictedFields();
		outputFields = evaluator.getOutputFields();

		/*
		* Define input fields. 
		* Filter outs auto-generated input fields in pmml, by selecting only the
		* fields that exist in the given siddhi query.
		*/
		for (FieldName field : allFields) {
			if (parameterPositions.keySet().contains(field.getValue())) {
				inputs.add(field);
			}
		}		
		this.outStreamDefinition = new StreamDefinition().name("pmmlPredictedStream");
		initializeOutputStream(model);
	}

	protected boolean isFilePath(String path) {
		File file = new File(path);
		if (file.exists() && !file.isDirectory()) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Extract the name and the data type of the output fields and predicted
	 * fields from the pmml
	 * and initialize an output stream having attributes with same name and data
	 * type
	 */
	protected void initializeOutputStream(PMML model) {
		for (FieldName predictedField : predictedFields) {
			String dataType = evaluator.getDataField(predictedField).getDataType().toString();
			Attribute.Type type = null;
			if (dataType.equalsIgnoreCase("double")) {
				type = Attribute.Type.DOUBLE;
			} else if (dataType.equalsIgnoreCase("float")) {
				type = Attribute.Type.FLOAT;
			} else if (dataType.equalsIgnoreCase("integer")) {
				type = Attribute.Type.INT;
			} else if (dataType.equalsIgnoreCase("long")) {
				type = Attribute.Type.LONG;
			} else if (dataType.equalsIgnoreCase("string")) {
				type = Attribute.Type.STRING;
			} else if (dataType.equalsIgnoreCase("boolean")) {
				type = Attribute.Type.BOOL;
			}
			this.outStreamDefinition.attribute(predictedField.toString(), type);
		}
		for (FieldName outputField : outputFields) {
			DataType dataType = evaluator.getOutputField(outputField).getDataType();
			if (dataType == null) {
				dataType = evaluator.getDataField(predictedFields.get(0)).getDataType();
			}
			Attribute.Type type = null;
			if (dataType.toString().equalsIgnoreCase("double")) {
				type = Attribute.Type.DOUBLE;
			} else if (dataType.toString().equalsIgnoreCase("float")) {
				type = Attribute.Type.FLOAT;
			} else if (dataType.toString().equalsIgnoreCase("integer")) {
				type = Attribute.Type.INT;
			} else if (dataType.toString().equalsIgnoreCase("long")) {
				type = Attribute.Type.LONG;
			} else if (dataType.toString().equalsIgnoreCase("string")) {
				type = Attribute.Type.STRING;
			} else if (dataType.toString().equalsIgnoreCase("boolean")) {
				type = Attribute.Type.BOOL;
			}
			this.outStreamDefinition.attribute(outputField.toString(), type);
		}
	}

	@Override
	public void destroy() {
	}
}