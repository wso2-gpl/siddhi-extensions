/*
 * Copyright (C) 2015 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.wso2.siddhi.gpl.extensions.geo.internal.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class GeoOperation {
    public boolean point = false;
    protected Object data;
    public PreparedGeometry geometry = null;

    public void init(ExpressionExecutor[] attributeExpressionExecutors, int start, int end) {
        int position = start;
        if (attributeExpressionExecutors[position].getReturnType() == Attribute.Type.DOUBLE) {
            point = true;
            if (attributeExpressionExecutors[position + 1].getReturnType() != Attribute.Type.DOUBLE) {
                throw new ExecutionPlanCreationException("Longitude and Latitude must be provided as double values");
            }
            ++position;
        } else if (attributeExpressionExecutors[position].getReturnType() == Attribute.Type.STRING) {
            point = false;
        } else {
            throw new ExecutionPlanCreationException((position + 1) +
                    " parameter should be a string for a geometry or a double for a latitude");
        }
        ++position;
        if (position >= end) {
            return;
        }
        if (attributeExpressionExecutors[position].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanCreationException((position + 1) + " parameter should be a GeoJSON geometry string");
        }
        if (attributeExpressionExecutors[position] instanceof ConstantExpressionExecutor) {
            String strGeometry = attributeExpressionExecutors[position].execute(null).toString();
            geometry = GeometryUtils.preparedGeometryFromJSON(strGeometry);
        }
    }

    public Object process(Object[] data) {
        Geometry currentGeometry;
        if (point) {
            double longitude = (Double) data[0];
            double latitude = (Double) data[1];
            currentGeometry = GeometryUtils.createPoint(longitude, latitude);
        } else {
            currentGeometry = GeometryUtils.geometryFromJSON(data[0].toString());
        }
        if (geometry != null) {
            return operation(currentGeometry, geometry, data);
        } else {
            return operation(currentGeometry, GeometryUtils.geometryFromJSON(data[point ? 2 : 1].toString()),
                    data);
        }
    }

    public Geometry getCurrentGeometry(Object[] data) {
        if (point) {
            double longitude = (Double) data[0];
            double latitude = (Double) data[1];
            return GeometryUtils.createPoint(longitude, latitude);
        } else {
            return GeometryUtils.createGeometry(data[0]);
        }
    }

    public abstract Object operation(Geometry a, Geometry b, Object[] data);

    public abstract Object operation(Geometry a, PreparedGeometry b, Object[] data);

    public abstract Attribute.Type getReturnType();
}
