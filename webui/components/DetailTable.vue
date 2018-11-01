<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
  <div>
    <el-table
      border
      empty-text="No data"
      :row-class-name="_rowClassName"
      :data="tableData">
      <el-table-column type="expand">
        <template slot-scope="props">
          <ul>
            <li v-for="ep in props.row.extra" :key="ep.key">
              {{ _preprocessKey(ep.key) }}: {{ ep.value }}
            </li>
          </ul>
        </template>
      </el-table-column>
      <el-table-column label="Key" prop="key"/>
      <el-table-column label="Value" prop="value"/>
    </el-table>
  </div>
</template>

<script>
export default {
  props: ['tableData'],

  methods: {
    _rowClassName(rowObject) {
      if (rowObject.row.extra) {
        return '';
      }
      return 'no-expand';
    },

    _preprocessKey(key) {
      if (/^.+\..+$/.test(key)) {
        const splitted = key.split('.');
        return splitted[splitted.length - 1];
      }
      return key;
    },
  },
}
</script>

<style>
.no-expand .el-icon {
  display: none;
}

.no-expand .el-table__expand-icon {
  pointer-events: none;
}
</style>
