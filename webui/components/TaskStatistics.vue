<!--
Copyright (C) 2018 Seoul National University
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<template>
  <el-card>
    <el-table
      v-if="columnArray.length !== 0"
      border
      :data="taskMetric"
      empty-text="No data">
      <el-table-column label="id" prop="id" sortable/>
      <el-table-column label="state" align="center">
        <template slot-scope="scope">
          <el-tag :type="toStateType(scope.row.stateTransitionEvents)">
            {{ toStateText(scope.row.stateTransitionEvents) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column
        v-for="col in columnArray"
        sortable
        :sort-method="(a, b) => _sortFunc(a, b, col)"
        :label="col"
        :key="col"
        :prop="col"/>
    </el-table>
  </el-card>
</template>

<script>
import { STATE } from '../assets/constants';

export const EXCLUDE_COLUMN = [
  'id',
  'state',
  'group',
  'start',
  'end',
  'stateTransitionEvents',
  'content',
  'metricId',
];

const NOT_AVAILABLE = -1;

// this function will preprocess TaskMetric metric array.
const _preprocessMetric = function (metric) {
  let newMetric = Object.assign({}, metric);

  Object.keys(newMetric).forEach(key => {
    // replace NOT_AVAILBLE to 'N/A'
    if (newMetric[key] === NOT_AVAILABLE) {
      newMetric[key] = 'N/A';
    }
  });

  if (newMetric.stateTransitionEvents) {
    const ste = newMetric.stateTransitionEvents;
    if (ste.length > 2) {
      const firstEvent = ste[0], lastEvent = ste[ste.length - 1];
      if (_isDoneTaskEvent(lastEvent)) {
        newMetric.duration = lastEvent.timestamp - firstEvent.timestamp;
      } else {
        newMetric.duration = 'N/A';
      }
    } else {
      newMetric.duration = 'N/A';
    }
  }

  return newMetric;
};

const _isDoneTaskEvent = function (event) {
  if (event.newState === STATE.COMPLETE
    || event.newState === STATE.FAILED) {
    return true;
  }
  return false;
};

export default {
  props: ['metricLookupMap'],

  methods: {
    _sortFunc(_a, _b, column) {
      let a = _a[column], b = _b[column];
      if (a === 'N/A') {
        return -1;
      } else if (b === 'N/A') {
        return 1;
      } else if (a === b) {
        return 0;
      } else if (a > b) {
        return 1;
      } else if (a < b) {
        return -1;
      }
      return 0;
    },

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

  computed: {
    /**
     * Computed property which consists table data.
     */
    taskMetric() {
      return Object.keys(this.metricLookupMap)
        .filter(key => this.metricLookupMap[key].group === 'TaskMetric')
        .map(key => _preprocessMetric(this.metricLookupMap[key]));
    },

    /**
     * Computed property of column string array.
     * This property will look first element of `taskMetric` array and
     * extract object keys and filter by EXECLUDE_COLUMN.
     */
    columnArray() {
      if (!this.taskMetric[0]) {
        return [];
      }

      return Object.keys(this.taskMetric[0])
        .filter(k => !EXCLUDE_COLUMN.includes(k));
    },
  },
}
</script>
