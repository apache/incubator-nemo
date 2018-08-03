<template>
  <el-card>
    <el-button
      :disabled="uploading"
      type="primary"
      plain
      @click="addJobDialogVisible = true">
      Add job
    </el-button>

    <el-table
      class="job-table"
      empty-text="No data"
      :data="jobTableData">
      <el-table-column label="From">
        <template slot-scope="scope">
          {{ _getFrom(scope.row.jobId) }}
        </template>
      </el-table-column>
      <el-table-column label="Status">
        <template slot-scope="scope">
          <el-tag :type="_fromJobStatusToType(scope.row.status)">
            {{ scope.row.status }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Operations">
        <template slot-scope="scope">
          <el-button
            @click="selectJobId(scope.row.jobId)"
            round
            type="primary">
            Select
          </el-button>
          <el-button
            @click="deleteJobId(scope.row.jobId)"
            circle
            type="danger"
            icon="el-icon-delete"/>
          <el-button
            v-if="_isWebSocketJob(scope.row.jobId)"
            @click="prepareWebSocket(scope.row.jobId)"
            :disabled="_reconnectDisabled(scope.row.status)"
            circle
            type="info"
            :icon="_reconnectIconType(scope.row.status)"/>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog
      title="Add job"
      :visible.sync="addJobDialogVisible"
      width="40%">
      <el-card class="dialog-card">
        <div slot="header">
          <h2>From JSON file</h2>
        </div>
        <el-row type="flex" justify="center">
          <el-upload
            drag
            :span="20"
            action=""
            :http-request="handleUpload"
            :auto-upload="true"
            :show-file-list="false">
            <i class="el-icon-upload"/>
            <div class="el-upload__text">
              Drop JSON here or <em>click here to upload.</em>
            </div>
          </el-upload>
        </el-row>
      </el-card>
      <el-card>
        <div slot="header">
          <h2>From WebSocket endpoint</h2>
        </div>
        <el-row class="dialog-row">
          <el-input
            clearable
            placeholder="WebSocket endpoint"
            v-model="wsEndpointInput"/>
        </el-row>
        <el-row class="dialog-row" type="flex" justify="end">
          <el-button
            type="primary"
            plain
            @click="handleWebSocketAdd">
            Add
          </el-button>
        </el-row>
      </el-card>
    </el-dialog>
  </el-card>
</template>

<script>
import Vue from 'vue';
import uuid from 'uuid/v4';
import { DataSet } from 'vue2vis';
import { STATE } from '~/assets/constants';

const JOB_STATUS = {
  NOT_CONNECTED: 'NOT CONNECTED',
  CONNECTING: 'CONNECTING',
  RUNNING: 'RUNNING',
  COMPLETE: 'COMPLETE',
  FAILED: 'FAILED',
};

function _isDone(status) {
  return status === JOB_STATUS.COMPLETE ||
    status === JOB_STATUS.FAILED;
}

export default {
  data() {
    return {
      // job id -> job data object
      jobs: {},

      selectedJobId: '',

      // ui-specific
      addJobDialogVisible: false,
      uploading: false,

      wsEndpointInput: '',
    };
  },

  computed: {
    jobTableData() {
      return Object.keys(this.jobs).map(jobId => ({
        jobId: jobId,
        status: this.jobs[jobId].status,
      }));
    },
  },

  methods: {
    async handleUpload(options) {
      this.addJobDialogVisible = false;
      let fileReader = new FileReader();

      fileReader.onload = event => {
        this.uploading = false;
        this.addJobFromFile(options.file.name, event.target.result);
      };

      fileReader.onerror = event => {
        this.uploading = true;
        this.notifyError('File upload failed');
      };

      this.uploading = true;
      fileReader.readAsText(options.file);
    },

    notifyError(msg) {
      this.$notify.error({
        title: 'Error',
        message: msg,
      });
    },

    _getFrom(jobId) {
      const job = this.jobs[jobId];
      return job.endpoint ? job.endpoint : job.fileName;
    },

    _isWebSocketJob(jobId) {
      return this.jobs[jobId].endpoint ? true : false;
    },

    _fromJobStatusToType(status) {
      switch (status) {
        case JOB_STATUS.RUNNING:
          return 'primary';
        case JOB_STATUS.COMPLETE:
          return 'success';
        case JOB_STATUS.FAILED:
          return 'danger';
        default:
          return 'info';
      }
    },

    _reconnectDisabled(status) {
      switch (status) {
        case JOB_STATUS.NOT_CONNECTED:
          return false;
        case JOB_STATUS.CONNECTING:
          return true;
        default:
          return true;
      }
    },

    _reconnectIconType(status) {
      switch (status) {
        case JOB_STATUS.NOT_CONNECTED:
          return 'el-icon-refresh';
        case JOB_STATUS.CONNECTING:
          return 'el-icon-loading';
        default:
          return 'el-icon-refresh';
      }
    },

    async selectJobId(jobId) {
      if (!(jobId in this.jobs) || jobId === this.selectedJobId) {
        return;
      }

      this.selectedJobId = jobId;
      const job = this.jobs[jobId];
      this.$eventBus.$emit('job-id-select', {
        jobId,
        jobFrom: this._getFrom(jobId),
        metricLookupMap: job.metricLookupMap,
        metricDataSet: job.metricDataSet,
      });

      await this.$nextTick();

      if (job.dag) {
        this.$eventBus.$emit('dag', {
          dag: job.dag,
          jobId,
          init: true,
          states: job.dagStageState,
        });
      }
    },

    deleteJobId(jobId) {
      if (this.selectedJobId === jobId) {
        this.$eventBus.$emit('job-id-deselect');
        this.$eventBus.$emit('clear-dag');
      }
      const job = this.jobs[jobId];

      if (job.ws) {
        ws.close();
      }

      Vue.delete(this.jobs, jobId);
    },

    handleWebSocketAdd() {
      this.addJobFromWebSocketEndpoint(this.wsEndpointInput);
      this.wsEndpointInput = '';
      this.addJobDialogVisible = false;
    },

    _newJob(jobId) {
      Vue.set(this.jobs, jobId, {
        ws: undefined,
        endpoint: '',
        fileName: '',
        dag: undefined,
        metricLookupMap: {},
        metricDataSet: new DataSet([]),
        dagStageState: {},
        status: '',
      });
    },

    async addJobFromFile(fileName, content) {
      let parsedData;
      try {
        parsedData = JSON.parse(content);
      } catch (e) {
        this.notifyError('Invalid JSON file');
        return;
      }

      const jobId = uuid();
      this._newJob(jobId);
      this.jobs[jobId].fileName = fileName;
      this.jobs[jobId].status = JOB_STATUS.RUNNING;
      this.selectJobId(jobId);

      await this.$nextTick();
      this.processMetric(parsedData, jobId);
    },

    addJobFromWebSocketEndpoint(endpoint) {
      let alreadyExistsError = false;
      endpoint = endpoint.trim();

      Object.keys(this.jobs)
        .filter(k => _isDone(!this.jobs[k].status))
        .forEach(k => {
          if (this.jobs[k].endpoint === endpoint) {
            alreadyExistsError = true;
            return;
          }
        });

      if (alreadyExistsError) {
        this.notifyError('Endpoint already exists');
        return;
      }

      const jobId = uuid();
      this._newJob(jobId);
      this.jobs[jobId].endpoint = endpoint;
      this.jobs[jobId].status = JOB_STATUS.NOT_CONNECTED;
      this.selectJobId(jobId);

      this.prepareWebSocket(jobId);
    },

    prepareWebSocket(jobId) {
      if (!process.browser) {
        return;
      }

      const job = this.jobs[jobId];

      if (job.ws && job.ws.readyState !== WebSocket.CLOSED) {
        return;
      }

      job.ws = new WebSocket(job.endpoint);
      job.status = JOB_STATUS.CONNECTING;

      job.ws.onopen = () => {
        job.status = JOB_STATUS.RUNNING;
      };

      job.ws.onmessage = (event) => {
        let parsedData;
        try {
          parsedData = JSON.parse(event.data);
        } catch (e) {
          console.warn('Non-JSON data received: ' + jobId);
          return;
        }

        // pass to metric handling logic
        this.processMetric(parsedData, jobId);
      };

      job.ws.onclose = () => {
        if (job.ws) {
          job.ws = undefined;
          if (job.status === JOB_STATUS.CONNECTING) {
            job.status = JOB_STATUS.NOT_CONNECTED;
          }
        }
      };

      window.onbeforeunload = () => {
        if (job.ws) {
          job.ws.close();
        }
      };
    },

    async processMetric(metric, jobId) {
      // specific event broadcast
      if ('metricType' in metric) {
        await this.processIndividualMetric(metric, jobId);
      } else {
        // the first big metric chunk
        Object.keys(metric).forEach(metricType => {
          Object.values(metric[metricType]).forEach(async chunk => {
            await this.processIndividualMetric({
              metricType: metricType,
              data: chunk.data,
            }, jobId);
          });
        });
      }
    },

    async processIndividualMetric({ metricType, data }, jobId) {
      const job = this.jobs[jobId];

      let newItem = { group: metricType };
      // overwrite item object with received data
      Object.assign(newItem, data);

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          const { prevState, newState, timestamp } = event;

          let metricObj = {
            jobId,
            metricId: data.id,
            metricType,
            prevState,
            newState,
          };

          this.$eventBus.$emit('state-change-event', metricObj);
          this._cacheStageState(metricObj);

          if (metricType === 'JobMetric') {
            // READY -> EXECUTING -> COMPLETE / FAILED
            switch (prevState) {
              case STATE.READY:
                newItem.start = new Date(timestamp);
                break;
            }
            switch (newState) {
              case STATE.COMPLETE:
                job.status = JOB_STATUS.COMPLETE;
                newItem.end = new Date(timestamp);
                break;
              case STATE.FAILED:
                job.status = JOB_STATUS.FAILED;
                newItem.end = new Date(timestamp);
                break;
            }
          } else if (metricType === 'StageMetric') {
            // INCOMPLETE -> COMPLETE
            switch (newState) {
              case STATE.COMPLETE:
              case STATE.FAILED:
                // Stage does not have READY, so it cannot be
                // represented as a range of timeline.
                // So the only needed field is `start`.
                newItem.start = new Date(timestamp);
                break;
            }
          } else if (metricType === 'TaskMetric') {
            // READY -> EXECUTING -> (SHOULD_RETRY) -> COMPLETE / FAILED
            switch (prevState) {
              case STATE.READY:
                newItem.start = new Date(timestamp);
                break;
            }

            switch (newState) {
              case STATE.COMPLETE:
              case STATE.FAILED:
                newItem.end = new Date(timestamp);
                break;
            }
          }

          newItem.content = data.id;
        });

      // if data contains `dag`, it will send it to DAG component
      if (data.dag) {
        job.dag = data.dag;
        this.$eventBus.$emit('dag', {
          dag: data.dag,
          jobId: jobId,
          init: false,
          states: job.dagStageState,
        });
        this.buildMetricLookupMapWithDAG(jobId);
      }

      let prevItem = job.metricDataSet.get(newItem.id);
      if (!prevItem) {
        try {
          job.metricDataSet.add(newItem);
          this.addMetricToMetricLookupMap(newItem, jobId);
        } catch (e) {
          console.warn('Error when adding new item');
        }
        if (job.metricDataSet.length === 1) {
          this.moveTimeline(newItem.start, jobId);
        } else {
          this.fitTimeline(jobId);
        }
      } else {
        try {
          job.metricDataSet.update(newItem);
          this.addMetricToMetricLookupMap(newItem, jobId);
        } catch (e) {
          console.warn('Error when updating item');
        }
        if (!(prevItem.start === newItem.start && prevItem.end === newItem.end)) {
          this.fitTimeline(jobId);
        }
      }
    },

    _cacheStageState({ jobId, metricId, metricType, newState }) {
      if (metricType !== 'StageMetric') {
        return;
      }

      const job = this.jobs[jobId];
      job.dagStageState[metricId] = newState;
    },

    fitTimeline(jobId) {
      this.$eventBus.$emit('fit-timeline', jobId);
    },

    moveTimeline(time, jobId) {
      this.$eventBus.$emit('move-timeline', {
        jobId: jobId,
        time: time,
      });
    },

    _flatten(metric) {
      let newMetric = {};
      Object.keys(metric).forEach(key => {
        if (key === 'properties') {
          Object.assign(newMetric, this._flatten(metric[key]));
        } else if (key !== 'irDag') {
          newMetric[key] = metric[key];
        }
      });

      return newMetric;
    },

    buildMetricLookupMapWithDAG(jobId) {
      const job = this.jobs[jobId];
      job.dag.vertices.forEach(stage => {
        Vue.set(job.metricLookupMap, stage.id, this._flatten(stage));
        stage.properties.irDag.vertices.forEach(vertex => {
          Vue.set(job.metricLookupMap, vertex.id, this._flatten(vertex));
        });
        stage.properties.irDag.edges.forEach(edge => {
          const edgeId = edge.properties.runtimeEdgeId;
          Vue.set(job.metricLookupMap, edgeId, this._flatten(edge));
        });
      });
      job.dag.edges.forEach(edge => {
        const edgeId = edge.properties.runtimeEdgeId;
        Vue.set(job.metricLookupMap, edgeId, this._flatten(edge));
      });
    },

    addMetricToMetricLookupMap(metric, jobId) {
      const job = this.jobs[jobId];
      if (metric.group === 'JobMetric') {
        Vue.set(job.metricLookupMap, metric.id, metric);
      } else if (metric.group === 'TaskMetric') {
        Vue.set(job.metricLookupMap, metric.id, metric);
      }
      this.$eventBus.$emit('build-table-data', {
        metricId: metric.id,
        jobId: jobId,
      });
    },
  },
}
</script>
<style>
.job-table {
  margin-top: 20px;
}

.dialog-card {
  margin-bottom: 20px;
}

.dialog-row {
  padding: 10px;
}
</style>
