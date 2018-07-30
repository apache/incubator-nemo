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
      border
      :data="jobTableData">
      <el-table-column label="Job ID" prop="jobId"/>
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

export default {
  // TODO: manage multiple WebSocket object
  // and also multiple endpoints
  // TODO: upload json dump file
  data() {
    return {
      // job id -> job data object
      jobs: {
        'job-1': {
          ws: undefined,
          endpoint: '',
          dag: undefined,
          metricLookupMap: {},
          metricDataSet: undefined,
          completed: false,
        }
      },

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
        throw new Error('error when uploading file');
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

    selectJobId(jobId) {
      this.selectedJobId = jobId;
      const job = this.jobs[jobId];
      this.$eventBus.$emit('job-id-select', {
        jobId: jobId,
        metricLookupMap: job.metricLookupMap,
        metricDataSet: job.metricDataSet,
      });
    },

    handleWebSocketAdd() {
      this.addJobFromWebSocketEndpoint(this.wsEndpointInput);
      this.wsEndpointInput = '';
    },

    _newJob(jobId) {
      let newJobEntry = {
        ws: undefined,
        endpoint: '',
        dag: undefined,
        metricLookupMap: {},
        metricDataSet: new DataSet([]),
        completed: false,
      };

      Vue.set(this.jobs, jobId, newJobEntry);
    },

    addJobFromFile(fileName, content) {

    },

    addJobFromWebSocketEndpoint(endpoint) {
      let alreadyExistsError = false;
      endpoint = endpoint.trim();

      Object.keys(this.jobs)
        .filter(k => !this.jobs[k].completed)
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
      this.selectJobId(jobId);
      this.jobs[jobId].endpoint = endpoint;

      this.prepareWebSocket(jobId);
    },

    prepareWebSocket(jobId) {
      if (!process.browser) {
        return;
      }

      const job = this.jobs[jobId];

      if (job.ws && job.ws.readyState !== WebSocket.CLOSED) {
        // this.closeWebSocket();
        // is this really correct?
        return;
      }

      job.ws = new WebSocket(job.endpoint);

      job.ws.onopen = () => {
        // clear metric
        /*
        this.metricDataSet.clear();
        this.selectedMetricId = '';
        */
        job.wsStatus = 'opened';
      };

      job.ws.onmessage = (event) => {
        let parsedData;
        try {
          parsedData = JSON.parse(event.data);
        } catch (e) {
          console.warn('Non-JSON data received - ' + jobId);
          return;
        }

        // pass to metric handling logic
        this.processMetric(parsedData, jobId);
      };

      job.ws.onclose = () => {
        job.wsStatus = 'closed';
        if (job.ws) {
          job.ws = undefined;
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
          Object.values(metric[metricType]).forEach(async data => {
            await this.processIndividualMetric({
              metricType: metricType,
              data: data,
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

      // if data contains `dag`, it will send to DAG component
      // TODO: support multi job with job identifier
      // maybe can use self-generated UUIDv4?
      if (data.dag) {
        job.dag = data.dag;
        this.$eventBus.$emit('dag', {
          dag: data.dag,
          jobId: jobId,
        });
        this.buildMetricLookupMapWithDAG(jobId);
      }

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          if (event.prevState === STATE.INCOMPLETE) {
            // Stage does not have READY, so it cannot be represented as
            // a range of timeline. So the only needed field is `start`.
            this.$eventBus.$emit('stage-event', {
              jobId: jobId,
              stageId: data.id,
              state: STATE.COMPLETE,
            });
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id + ' COMPLETE';
          } else if (event.prevState === STATE.READY) {
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id;
          } else if (event.newState === STATE.COMPLETE) {
            if (newItem.start) {
              newItem.end = new Date(event.timestamp);
            } else {
              newItem.start = new Date(event.timestamp);
            }
            newItem.content = data.id;
          }
        });

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
