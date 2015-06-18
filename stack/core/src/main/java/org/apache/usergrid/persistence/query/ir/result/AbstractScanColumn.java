/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.persistence.query.ir.result;


import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.utils.ByteBufferUtil;


/**
 *
 * @author: tnine
 *
 */
public abstract class AbstractScanColumn implements ScanColumn {

    protected final UUID uuid;
    protected final ByteBuffer buffer;
    protected ScanColumn child;


    protected AbstractScanColumn( final UUID uuid, final ByteBuffer columnNameBuffer ) {
        this.uuid = uuid;
        this.buffer = columnNameBuffer;
    }


    @Override
    public UUID getUUID() {
        return uuid;
    }


    @Override
    public ByteBuffer getCursorValue() {
        return buffer == null ? null : buffer.duplicate();
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof AbstractScanColumn ) ) {
            return false;
        }

        AbstractScanColumn that = ( AbstractScanColumn ) o;

        return uuid.equals( that.uuid );
    }


    @Override
    public int hashCode() {
        return uuid.hashCode();
    }


    @Override
    public String toString() {
        return "AbstractScanColumn{" +
                "uuid=" + uuid +
                ", buffer=" + ByteBufferUtil.bytesToHex( buffer ) +
                '}';
    }


    @Override
    public void setChild( final ScanColumn childColumn ) {
        this.child = childColumn;
    }


    @Override
    public ScanColumn getChild() {
        return child;
    }


    /**
     * Comparator for comparing children.  A null safe call
     * @param otherScanColumn
     * @return
     */
    protected int compareChildren( final ScanColumn otherScanColumn ) {

        if ( otherScanColumn == null ) {
            return 1;
        }

        final ScanColumn otherChild = otherScanColumn.getChild();

        if ( child != null && otherChild != null ) {
            return child.compareTo( otherChild );
        }

        return 0;
    }
}
