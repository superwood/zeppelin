/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.druid;

import com.google.common.base.Function;
import com.yahoo.sql4d.sql4ddriver.DDataSource;
import com.yahoo.sql4d.sql4ddriver.Joiner4All;
import com.yahoo.sql4d.sql4ddriver.Mapper4All;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * based on driver docs here:
 * <p>
 * https://github.com/srikalyc/Sql4D/wiki/Sql4DDriver
 */
public class DruidSqlInterpreter extends Interpreter {
  private Logger logger = LoggerFactory.getLogger(DruidSqlInterpreter.class);

  static final String DRUID_OVERLORD_HOST = "druid.overlord.host";
  static final String DRUID_OVERLORD_PORT = "druid.overlord.port";
  static final String DRUID_COORDINATOR_HOST = "druid.coordinator.host";
  static final String DRUID_COORDINATOR_PORT = "druid.coordinator.port";
  static final String DRUID_BROKER_HOST = "druid.broker.host";
  static final String DRUID_BROKER_PORT = "druid.broker.port";

  static {
    Interpreter.register(
        "druid",
        "druid",
        DruidSqlInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
                 .add(DRUID_OVERLORD_HOST, "localhost", "The Druid Overlord Host Name")
                        .add(DRUID_OVERLORD_PORT, "8080", "The Druid Overlord Port")
                        .add(DRUID_BROKER_HOST, "localhost", "The Druid Broker Host Name")
                        .add(DRUID_BROKER_PORT, "8080", "The Druid Broker Port")
                        .add(DRUID_COORDINATOR_HOST, "localhost", "The Druid Coordinator Host Name")
                        .add(DRUID_COORDINATOR_PORT, "8080", "The Druid Coordinator Port")
                        .build());
  }


  private static DDataSource dDriver;

  public DruidSqlInterpreter(Properties property) {
      super(property);
  }

  @Override
  public void open() {
    String brokerHost = getProperty(DRUID_BROKER_HOST);
    Integer brokerPort = Integer.valueOf(getProperty(DRUID_BROKER_PORT));
    String coordinatorHost = getProperty(DRUID_COORDINATOR_HOST);
    Integer coordinatorPort = Integer.valueOf(getProperty(DRUID_COORDINATOR_PORT));
    String overlordHost = getProperty(DRUID_OVERLORD_HOST);
    Integer overlordPort = Integer.valueOf(getProperty(DRUID_OVERLORD_PORT));

    dDriver = new DDataSource(brokerHost, brokerPort,
            coordinatorHost, coordinatorPort, overlordHost, overlordPort);
  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    return executeSql(st);
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

  private InterpreterResult executeSql(String sql) {
    try {
      Either<String, Either<Joiner4All, Mapper4All>> result = dDriver.query(sql, null);
      if (result.isLeft()) {
        return new InterpreterResult(Code.ERROR, result.left().get().toString());
      }
      Either<Joiner4All, Mapper4All> goodResult =
          (Either<Joiner4All, Mapper4All>) result.right().get();
      if (goodResult.isLeft()) {
        return new InterpreterResult(Code.SUCCESS, goodResult.left().get().toString());
      } else {
        return new InterpreterResult(Code.SUCCESS,
          mapper4All2Zeppelin((Mapper4All) goodResult.right().get()));
      }

    } catch (Exception e) {
      return new InterpreterResult(Code.ERROR, e.getMessage());
    }

  }

  private String mapper4All2Zeppelin(Mapper4All mapper4All) {
    StringBuilder msg = new StringBuilder("%table\n");
    msg.append(StringUtils.join(mapper4All.baseFieldNames, "\t") + "\n");
    for (List<Object> row : mapper4All.baseAllRows) {
      msg.append(StringUtils.join(row, "\t") + "\n");
    }
    return msg.toString();
  }
}
