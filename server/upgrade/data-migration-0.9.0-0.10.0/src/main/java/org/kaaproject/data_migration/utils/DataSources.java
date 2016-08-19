/*
 * Copyright 2014-2016 CyberVision, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kaaproject.data_migration.utils;

import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;



final public class DataSources {

    public static DataSource getPostgreSQL(Options opt) {
        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName("org.postgresql.Driver");
        bds.setUrl("jdbc:postgresql://" + opt.getHost() + ":5432/" + opt.getDbName());
        bds.setUsername(opt.getUsername());
        bds.setPassword(opt.getPassword());
        bds.setDefaultAutoCommit(false);
        return bds;
    }


    public static DataSource getMariaDB(Options opt) {
        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName("org.mariadb.jdbc.Driver");
        bds.setUrl("jdbc:mysql://" + opt.getHost() + ":3306/" + opt.getDbName());
        bds.setUsername(opt.getUsername());
        bds.setPassword(opt.getPassword());
        bds.setDefaultAutoCommit(false);
        return bds;
    }
}