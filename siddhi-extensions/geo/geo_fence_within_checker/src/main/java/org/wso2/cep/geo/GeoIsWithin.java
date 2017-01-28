package org.wso2.cep.geo;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

@SiddhiExtension(namespace = "geo", function = "iswithin")
public class GeoIsWithin extends FunctionExecutor {

	Logger log = Logger.getLogger(GeoIsWithin.class);
	private GeometryFactory geometryFactory;
	private Polygon polygon;

	@Override
	public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
		if (types.length != 3) {
			throw new QueryCreationException(
					"Not enough number of method arguments");
		} else {
			if(types[0] != Attribute.Type.DOUBLE || types[1] != Attribute.Type.DOUBLE)
				throw new QueryCreationException("lattitude and longitude must be provided as double values");
			if(types[2] != Attribute.Type.STRING)
				throw new QueryCreationException("polygon parameter should be a geojson feature string");
			String strPolygon = (String) attributeExpressionExecutors.get(2)
					.execute(null);
			JsonObject jsonObject = new JsonParser().parse(strPolygon)
					.getAsJsonObject();
			geometryFactory = JTSFactoryFinder.getGeometryFactory();
			JsonArray jLocCoordinatesArray = (JsonArray) jsonObject
					.getAsJsonArray("coordinates").get(0);
			Coordinate[] coords = new Coordinate[jLocCoordinatesArray.size()];
			for (int i = 0; i < jLocCoordinatesArray.size(); i++) {
				JsonArray jArray = (JsonArray) jLocCoordinatesArray.get(i);
				coords[i] = new Coordinate(Double.parseDouble(jArray.get(0)
						.toString()), Double.parseDouble(jArray.get(1)
						.toString()));
			}
			LinearRing ring = geometryFactory.createLinearRing(coords);
			LinearRing holes[] = null; // use LinearRing[] to represent holes
			polygon = geometryFactory.createPolygon(ring, holes);
		}
	}

	@Override
	protected Object process(Object obj) {
		Object functionParams[] = (Object[]) obj;
		double lattitude = (Double) functionParams[0];
		double longitude = (Double) functionParams[1];
		/* Creating a point */
		Coordinate coord = new Coordinate(lattitude, longitude);
		Point point = geometryFactory.createPoint(coord);

		return point.within(polygon);
	}
	public Type getReturnType() {
		return Attribute.Type.BOOL;
	}
	public void destroy() {
	}
}
