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

package org.wso2.gpl.siddhi.extensions.geo.internal.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import org.wso2.siddhi.query.api.definition.Attribute;

public class ClosestOperation extends org.wso2.gpl.siddhi.extensions.geo.internal.util.GeoOperation {
    @Override
    public Object operation(Geometry a, Geometry b, Object[] data) {
        DistanceOp distOp = new DistanceOp(a, b);
        return distOp.nearestPoints();
    }

    @Override
    public Object operation(Geometry a, PreparedGeometry b, Object[] data) {
        DistanceOp distOp = new DistanceOp(a, b.getGeometry());
        return distOp.nearestPoints();
    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }
}
