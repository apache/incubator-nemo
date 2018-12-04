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
  <el-card>
    <el-table
      border
      :data="taskMetric"
      empty-text="No data">
      <el-table-column label="ID" prop="id"/>
      <el-table-column label="State" align="center">
        <template slot-scope="scope">
          <el-tag :type="toStateType(scope.row.stateTransitionEvents)">
            {{ toStateText(scope.row.stateTransitionEvents) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column
        v-for="col in Object.keys(columns)"
        :label="columns[col]"
        :key="col"
        :prop="col"/>
    </el-table>
    <el-pagination :page-size="pageSize" :total="total" :current-page.sync="currentPage"></el-pagination>
  </el-card>
</template>

<script>
import { STATE } from '../assets/constants';

const COLUMNS = {
  'serializedReadBytes': 'Bytes read (compressed)',
  'encodedReadBytes': 'Bytes read',
  'writtenBytes': 'Bytes written',
  'boundedSourceReadTime': 'Source read time',
  'containerId': 'Container ID',
}

const TASKS_PER_PAGE = 20

export default {
  props: ['taskStatistics'],

  data: function() {
    return { 
      columns: COLUMNS,
      pageSize: TASKS_PER_PAGE,
      currentPage: 1.
    };
  },

  //COMPUTED
  computed: {
    total() {
      return this.taskStatistics.tableView.length
    },

    /**
     * Computed property which consists table data.
     */
    taskMetric() {
      return this.taskStatistics.tableView.slice((this.currentPage - 1) * TASKS_PER_PAGE, this.currentPage * TASKS_PER_PAGE)
    },
  },

  //METHODS
  methods: {
    /**
     * Fetch last stateTransitionEvents element and extract
     * newState property. See STATE constant.
     * @returns newState of last stateTransitionEvents array
     */
    toStateText(e) {
      return e[e.length - 1].newState;
    },

    /**
     * Helper function for converting state to element-ui type string.
     * @param e state.
     * @returns element-ui type string.
     */
    toStateType(e) {
      let t = this.toStateText(e);
      switch (t) {
        case STATE.READY:
        case STATE.INCOMPLETE:
          return 'info';
        case STATE.EXECUTING:
          return '';
        case STATE.COMPLETE:
          return 'success';
        case STATE.FAILED:
          return 'danger';
        case STATE.ON_HOLD:
        case STATE.SHOULD_RETRY:
          return 'warning';
        default:
          return '';
      }
    }
  },


}
</script>
