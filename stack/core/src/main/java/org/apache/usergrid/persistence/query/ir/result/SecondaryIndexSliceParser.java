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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.uuid.UUIDComparator;

import me.prettyprint.hector.api.beans.DynamicComposite;


/**
 * Parser for reading and writing secondary index composites.  Instances of this class should not be shared among
 * iterators.
 *
 * It it designed with the following assumptions in mind.
 *
 * 1) The slice contains the same data type for every element 2) Evaluating the first parse call for a comparator is
 * sufficient for subsequent use
 *
 * @author tnine
 */
public class SecondaryIndexSliceParser implements SliceParser {

    //the type comparator
    private Comparator<Object> typeComparator;


    public SecondaryIndexSliceParser() {}


    /* (non-Javadoc)
     * @see org.apache.usergrid.persistence.query.ir.result.SliceParser#parse(java.nio.ByteBuffer)
     */
    @Override
    public ScanColumn parse( ByteBuffer buff, final boolean isReversed ) {
        final DynamicComposite composite = DynamicComposite.fromByteBuffer( buff.duplicate() );

        final UUID uuid = ( UUID ) composite.get( 2 );
        final Object value = composite.get( 1 );

        if ( typeComparator == null ) {
            typeComparator = getTypeComparator( value, isReversed );
        }

        return new SecondaryIndexColumn( uuid, value, buff, typeComparator );
    }


    private Comparator<Object> getTypeComparator( final Object value, final boolean isReversed ) {

        final Class clazz = value.getClass();
        final Comparator<Object> comparator = COMPARATOR_MAP.get( new MapKey( clazz, isReversed ) );

        if ( comparator == null ) {
            throw new NullPointerException( "comparator was not found for runtime type '" + clazz + "'" );
        }

        return comparator;
    }


    /**
     * Column for our secondary index type
     */
    public static class SecondaryIndexColumn extends AbstractScanColumn {

        private final Object value;
        private final Comparator<Object> valueComparator;


        /**
         * Create the secondary index column
         *
         * @param valueComparator The comparator for the values
         */
        public SecondaryIndexColumn( final UUID uuid, final Object value, final ByteBuffer columnNameBuffer,
                                     final Comparator<Object> valueComparator ) {
            super( uuid, columnNameBuffer );
            this.value = value;
            this.valueComparator = valueComparator;
        }


        /** Get the value from the node */
        public Object getValue() {
            return this.value;
        }


        @Override
        public int compareTo( final ScanColumn other ) {
            if ( other == null ) {
                return 1;
            }

            final int compare =  valueComparator.compare( value, ( ( SecondaryIndexColumn ) other ).value );

            //same value, compare uuids
            if(compare == 0){
                return com.fasterxml.uuid.UUIDComparator.staticCompare( uuid, ( ( SecondaryIndexColumn ) other ).uuid );
            }

            return compare;
        }
    }




    private static final Map<MapKey, Comparator<Object>> COMPARATOR_MAP = new HashMap<MapKey, Comparator<Object>>();

    static {

        final LongComparator longComparator = new LongComparator();
        COMPARATOR_MAP.put( new MapKey( Long.class, false ), longComparator );
        COMPARATOR_MAP.put( new MapKey( Long.class, true ), new ReverseComparator( longComparator ) );

        final StringComparator stringComparator = new StringComparator();

        COMPARATOR_MAP.put( new MapKey( String.class, false ), stringComparator );
        COMPARATOR_MAP.put( new MapKey( String.class, true ), new ReverseComparator( stringComparator ) );


        final UUIDComparator uuidComparator = new UUIDComparator();

        COMPARATOR_MAP.put( new MapKey( UUID.class, false ), uuidComparator );
        COMPARATOR_MAP.put( new MapKey( UUID.class, true ), new ReverseComparator( uuidComparator ) );
    }


    /**
     * The key for the map
     */
    private static final class MapKey {
        public final Class<?> clazz;
        public final boolean reversed;


        private MapKey( final Class<?> clazz, final boolean reversed ) {
            this.clazz = clazz;
            this.reversed = reversed;
        }


        @Override
        public boolean equals( final Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( !( o instanceof MapKey ) ) {
                return false;
            }

            final MapKey mapKey = ( MapKey ) o;

            if ( reversed != mapKey.reversed ) {
                return false;
            }
            return clazz.equals( mapKey.clazz );
        }


        @Override
        public int hashCode() {
            int result = clazz.hashCode();
            result = 31 * result + ( reversed ? 1 : 0 );
            return result;
        }
    }




    private static final class LongComparator implements Comparator<Object> {

        @Override
        public int compare( final Object first, final Object second ) {
            return Long.compare( ( Long ) first, ( Long ) second );
        }
    }


    private static final class StringComparator implements Comparator<Object> {
        @Override
        public int compare( final Object first, final Object second ) {
            if ( first == null && second != null ) {
                return -1;
            }

            return ( ( String ) first ).compareTo( ( String ) second );
        }
    }


    private static final class UUIDComparator implements Comparator<Object> {
        @Override
        public int compare( final Object first, final Object second ) {
            return ( ( UUID ) first ).compareTo( ( UUID ) second );
        }
    }


    /**
     * Reversed our comparator
     */
    private static final class ReverseComparator implements Comparator<Object> {

        private final Comparator<Object> comparator;


        private ReverseComparator( final Comparator<Object> comparator ) {this.comparator = comparator;}


        @Override
        public int compare( final Object first, final Object second ) {
            return comparator.compare( first, second ) * -1;
        }
    }
}
