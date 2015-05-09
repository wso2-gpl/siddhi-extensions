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
import org.wso2.siddhi.query.api.definition.Attribute;

public class IntersectsOperation extends GeoOperation {
    @Override
    public Object operation(Geometry a, Geometry b, Object[] data) {
        return a.intersects(b);
    }

    @Override
    public Object operation(Geometry a, PreparedGeometry b, Object[] data) {
        return b.intersects(a);
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }
}
