/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.persistence.index.impl;


import java.util.Map;

import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.index.IndexEdge;
import org.apache.usergrid.persistence.index.SearchEdge;
import org.apache.usergrid.persistence.index.SearchType;
import org.apache.usergrid.persistence.model.entity.Entity;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static org.apache.usergrid.persistence.index.impl.IndexingUtils.createContextName;
import static org.apache.usergrid.persistence.index.impl.IndexingUtils.idString;


/**
 * Represent the properties required to build an index request
 */
@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class" )
public class IndexRequest implements BatchRequest {

    public String writeAlias;
    public String documentId;

    public Map<String, Object> data;

    public IndexRequest( final String writeAlias, final ApplicationScope applicationScope, IndexEdge indexEdge, Entity entity) {
        this(writeAlias,IndexingUtils.createIndexDocId(applicationScope, entity,indexEdge), EntityToMapConverter.convert(applicationScope,indexEdge, entity));

    }

    public IndexRequest( final String writeAlias, String documentId,  Map<String, Object> data) {
        this.writeAlias = writeAlias;
        this.data = data;
        this.documentId = documentId;
    }

    /**
     * DO NOT DELETE!  Required for Jackson
     */
    public IndexRequest() {
    }


    public void doOperation( final Client client, final BulkRequestBuilder bulkRequest ) {
        IndexRequestBuilder builder = client.prepareIndex( writeAlias, IndexingUtils.ES_ENTITY_TYPE, documentId ).setSource( data );


        bulkRequest.add( builder );
    }



    @Override
    public boolean equals( final Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        final IndexRequest that = ( IndexRequest ) o;

        if ( !data.equals( that.data ) ) {
            return false;
        }
        if ( !documentId.equals( that.documentId ) ) {
            return false;
        }

        if ( !writeAlias.equals( that.writeAlias ) ) {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        int result = writeAlias.hashCode();
        result = 31 * result + documentId.hashCode();
        result = 31 * result + data.hashCode();
        return result;
    }
}