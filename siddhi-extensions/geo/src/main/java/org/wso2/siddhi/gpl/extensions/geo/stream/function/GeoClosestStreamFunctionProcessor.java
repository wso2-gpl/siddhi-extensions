package org.wso2.siddhi.gpl.extensions.geo.stream.function;

import com.vividsolutions.jts.geom.Coordinate;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.ClosestOperation;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.GeoOperation;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

public class GeoClosestStreamFunctionProcessor extends StreamFunctionProcessor {

    GeoOperation geoOperation;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.geoOperation = new ClosestOperation();
        this.geoOperation.init(attributeExpressionExecutors, 0, attributeExpressionExecutors.length);
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>(4);
        attributeList.add(new Attribute("geo1latitude", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("geo1longitude", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("geo2latitude", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("geo2longitude", Attribute.Type.DOUBLE));
        return attributeList;
    }

    @Override
    protected Object[] process(Object[] data) {
        Coordinate[] coordinates = (Coordinate[]) geoOperation.process(data);

        return new Object[]{coordinates[0].x, coordinates[0].y, coordinates[1].x, coordinates[1].y};
    }

    @Override
    protected Object[] process(Object o) {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
