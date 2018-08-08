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
];

const NOT_AVAILABLE = -1;

let _preprocessMetric = function (metric) {
  let newMetric = Object.assign({}, metric);

  Object.keys(newMetric).forEach(key => {
    if (newMetric[key] === NOT_AVAILABLE) {
      newMetric[key] = 'N/A';
    }
  });

  return newMetric;
}

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

    toStateText(e) {
      return e[e.length - 1].newState;
    },

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
    taskMetric() {
      return Object.keys(this.metricLookupMap)
        .filter(key => this.metricLookupMap[key].group === 'TaskMetric')
        .map(key => _preprocessMetric(this.metricLookupMap[key]));
    },

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
