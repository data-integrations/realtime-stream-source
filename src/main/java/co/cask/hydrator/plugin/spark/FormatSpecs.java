/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.data.schema.Schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Specification for a RecordFormat, including the class, schema, and settings to use for the format.
 */
public class FormatSpecs implements Serializable {

  private final String name;
  private final Schema schema;
  private final Map<String, String> settings;

  public FormatSpecs(String name, @Nullable Schema schema, @Nullable Map<String, String> settings) {
    this.name = name;
    this.schema = schema;
    this.settings = settings == null ? Collections.<String, String>emptyMap() : settings;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public Map<String, String> getSettings() {
    return settings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FormatSpecs)) {
      return false;
    }

    FormatSpecs that = (FormatSpecs) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(schema, that.schema) &&
      Objects.equals(settings, that.settings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, schema, settings);
  }

  @Override
  public String toString() {
    return "FormatSpecs{" +
      "name='" + name + '\'' +
      ", schema=" + schema +
      ", settings=" + settings +
      '}';
  }
}
