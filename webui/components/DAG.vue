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
  <div ref="canvasContainer" class="dag-canvas-container">
    <p v-if="!dag">DAG is not ready.</p>
    <div v-show="dag">
      <canvas ref="dagCanvas" class="dag-canvas" id="dag-canvas"></canvas>
    </div>
  </div>
</template>

<script>
import { fabric } from 'fabric';
import { Graph } from '@dagrejs/graphlib';
import graphlib from '@dagrejs/graphlib';
import dagre from 'dagre';
import { STATE } from '../assets/constants';

const DEBOUNCE_INTERVAL = 200;

const VERTEX_WIDTH = 50;
const VERTEX_HEIGHT = 30;
const VERTEX_RADIUS = 6;
const PAN_MARGIN = 20;
const RECT_ROUND_RADIUS = 4;
const RECT_STROKE_WIDTH = 2;
const EDGE_STROKE_WIDTH = 2;
const ARROW_SIDE = 3;
const FONT_SIZE = 2;

const GRAPH_MARGIN = 15;

const SUCCESS_COLOR = '#67C23A';
const DANGER_COLOR = '#F56C6C';

const BACKGROUND_COLOR = '#F2F6FC';
const CANVAS_RATIO = 0.75;
const MAX_ZOOM = 20;
const MIN_ZOOM = 0.1;
const TARGET_FIND_TOLERANCE = 4;

const MY_TAB_INDEX = '2';

const LISTENING_EVENT_LIST = [
  'resize-canvas',
  'rerender-dag',
  'metric-select-done',
  'metric-deselect-done',
  'clear-dag',
  'dag',
  'state-change-event',
];

export default {

  props: ['selectedJobId', 'tabIndex'],

  mounted() {
    this.initializeCanvas();
    this.setUpEventListener();
  },

  beforeDestroy() {
    LISTENING_EVENT_LIST.forEach(e => {
      this.$eventBus.$off(e);
    });
  },

  data() {
    return {
      canvas: undefined,
      fitCanvasNextTime: true,

      resizeDebounceTimer: undefined,

      dag: undefined,
      objectSelected: false,

      stageGraph: undefined,
      verticesGraph: {},

      // vertexId -> fabric.Circle of vertex
      vertexObjects: {},
      // stageId -> fabric.Rect of stage
      stageObjects: {},
      // stageId -> inner objects of stage (edges, vertices, text)
      stageInnerObjects: {},
      stageInnerTextObjects: {},
      // array of stage label text object
      stageTextObjects: [],
      // array of stage edge label text object
      stageEdgeTextObjects: [],
    };
  },

  computed: {

    /**
     * width of DAG
     */
    dagWidth() {
      if (!this.stageGraph) {
        return undefined;
      }

      const edges = this.stageGraph.edges();
      const stages = this.stageGraph.nodes();

      const edgesXCoords = edges
        .map(e => this.stageGraph.edge(e))
        .map(edge => edge.points)
        .reduce((acc, curr) => acc.concat(curr))
        .map(point => point.x)

      const stagesXCoordsRight = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.x + stage.width / 2)
      const stagesXCoordsLeft = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.x - stage.width / 2)

      const coordMax = Math.max(...(edgesXCoords.concat(stagesXCoordsRight)));
      const coordMin = Math.min(...(edgesXCoords.concat(stagesXCoordsLeft)));

      return coordMax - coordMin + EDGE_STROKE_WIDTH * 2;
    },

    /**
     * height of DAG
     */
    dagHeight() {
      if (!this.stageGraph) {
        return undefined;
      }
      const edges = this.stageGraph.edges();
      const stages = this.stageGraph.nodes();

      const edgesYCoords = edges
        .map(e => this.stageGraph.edge(e))
        .map(edge => edge.points)
        .reduce((acc, curr) => acc.concat(curr))
        .map(point => point.y)

      const stagesYCoordsBottom = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.y + stage.height / 2)
      const stagesYCoordsTop = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.y - stage.height / 2)
      const coordMax = Math.max(...(edgesYCoords.concat(stagesYCoordsBottom)));
      const coordMin = Math.min(...(edgesYCoords.concat(stagesYCoordsTop)));

      return coordMax - coordMin + EDGE_STROKE_WIDTH * 2;
    },

    /**
     * x coordinate of DAG viewport origin.
     */
    dagOriginX() {
      if (!this.stageGraph) {
        return undefined;
      }

      const edges = this.stageGraph.edges();
      const stages = this.stageGraph.nodes();

      const edgesXCoords = edges
        .map(e => this.stageGraph.edge(e))
        .map(edge => edge.points)
        .reduce((acc, curr) => acc.concat(curr))
        .map(point => point.x)

      const stagesXCoordsLeft = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.x - stage.width / 2)

      const coordMin = Math.min(...(edgesXCoords.concat(stagesXCoordsLeft)));

      return coordMin - EDGE_STROKE_WIDTH;
    },

    /**
     * y coordinate of DAG viewport origin.
     */
    dagOriginY() {
      if (!this.stageGraph) {
        return undefined;
      }
      const edges = this.stageGraph.edges();
      const stages = this.stageGraph.nodes();

      const edgesYCoords = edges
        .map(e => this.stageGraph.edge(e))
        .map(edge => edge.points)
        .reduce((acc, curr) => acc.concat(curr))
        .map(point => point.y)

      const stagesYCoordsTop = stages
        .map(node => this.stageGraph.node(node))
        .map(stage => stage.y - stage.height / 2)
      const coordMin = Math.min(...(edgesYCoords.concat(stagesYCoordsTop)));

      return coordMin - EDGE_STROKE_WIDTH;
    },

    /**
     * array of stage id inside of dag.
     */
    stageIdArray() {
      if (!this.dag) {
        return undefined;
      }
      return this.dag.vertices.map(v => v.id);
    },


    /**
     * array of stage edge id inside of dag.
     */
    stageEdges() {
      if (!this.dag) {
        return undefined;
      }
      return this.dag.edges;
    },

  },

  methods: {
    /**
     * Initialize canvas, which DAG will be drawn.
     */
    initializeCanvas() {
      this.canvas = new fabric.Canvas('dag-canvas', {
        selection: false,
        backgroundColor: BACKGROUND_COLOR,
        targetFindTolerance: TARGET_FIND_TOLERANCE,
        preserveObjectStacking: true,
      });
      this.canvas.renderAll();
    },

    /**
     * Initialize variables.
     * this method does not change the properties of canvas,
     * but resets DAG information and coordinates of mouse actions.
     */
    initializeVariables() {
      this.stageGraph = undefined;
      this.verticesGraph = {};
      this.vertexObjects = {};
      this.stageObjects = {};
      this.stageInnerObjects = {};
      this.stageInnerTextObjects = {};
      this.stageEdgeTextObjects = [];
      this.stageTextObjects = [];
      this.objectSelected = false;
    },

    /**
     * Resize canvas size according to outer element.
     * @param fit if true, DAG will be fitted after resize.
     * @param resizeToGraphSize if true, canvas height will be
     * resized to be fit with dag height.
     */
    resizeCanvas(fit, resizeToGraphSize=true) {
      return new Promise((resolve, reject) => {
        if (!this.canvas) {
          reject();
          return;
        }

        if (this.resizeDebounceTimer) {
          clearTimeout(this.resizeDebounceTimer);
        }

        this.resizeDebounceTimer = setTimeout(() => {
          if (this.$refs.canvasContainer) {
            const w = this.$refs.canvasContainer.offsetWidth;
            this.canvas.setWidth(w);
            if (resizeToGraphSize && this.stageGraph) {
              const targetHeight = (w / this.dagWidth) * this.dagHeight;
              this.canvas.setHeight(targetHeight);
            }
            if (fit) {
              this.fitCanvas();
            }
            resolve();
          }
        }, DEBOUNCE_INTERVAL);
      });
    },

    /**
     * Resize and fit the canvas. This method also recalculate offsets of
     * inner canvas and reset translate viewports to the last cached values.
     */
    async rerenderDAG() {
      // wait for tab pane render
      await this.$nextTick();
      await this.resizeCanvas(false);

      if (this.fitCanvasNextTime) {
        this.fitCanvasNextTime = false;
        await this.fitCanvas();
      }

      if (!this.dag) {
        return;
      }

      this.canvas.calcOffset();
      await this.$nextTick();
      this.canvas.renderAll();
      // maybe fabric.js issue?
      this.canvas.viewportTransform[4] = 0;
      this.canvas.viewportTransform[5] = 0;
      await this.fitCanvas();
    },

    /**
     * Setup all event listener for this component.
     */
    setUpEventListener() {
      // invoke resize of canvas when pages is resize
      if (process.browser) {
        window.addEventListener('resize',
          () => this.resizeCanvas(true), false);
      }

      this.$eventBus.$on('resize-canvas', () => {
        this.resizeCanvas(true);
      });

      // resize canvas, fit, and recalculate inner coordinates
      // and rerender canvas
      this.$eventBus.$on('rerender-dag', async () => {
        await this.rerenderDAG();
      });

      this.$eventBus.$on('metric-select-done', () => {
        if (this.tabIndex === MY_TAB_INDEX) {
          this.resizeCanvas(false);
        }
      });

      this.$eventBus.$on('metric-deselect-done', () => {
        if (this.tabIndex === MY_TAB_INDEX) {
          this.resizeCanvas(false);
        }
      });

      this.$eventBus.$on('clear-dag', () => {
        this.initializeVariables();
        this.fitCanvasNextTime = true;
        this.dag = null;
      });

      // new dag event
      this.$eventBus.$on('dag', async ({ dag, jobId, init, states }) => {
        if (jobId !== this.selectedJobId) {
          return;
        }

        if (init) {
          this.initializeVariables();
        }

        this.fitCanvasNextTime = true;

        this.setUpCanvasEventHandler();
        this.dag = dag;
        this.drawDAG();

        if (this.tabIndex === MY_TAB_INDEX) {
          await this.resizeCanvas(false);
        }

        this.setStates(states);
        // restore previous translation viewport
        this.canvas.viewportTransform[4] = 0;
        this.canvas.viewportTransform[5] = 0;
      });

      // stage state transition event
      this.$eventBus.$on('state-change-event', event => {
        const { jobId, metricId, metricType, newState } = event;
        // ignore if metric type is not StageMetric
        if (metricType !== 'StageMetric') {
          return;
        }

        // ignore if jobId mismatch with selected job
        if (jobId !== this.selectedJobId) {
          return;
        }

        // ignore if metric id is not included in inner stage objects
        if (!metricId || !(metricId in this.stageObjects)) {
          return;
        }

        const stage = this.stageObjects[metricId];

        if (newState === STATE.COMPLETE) {
          stage.set('fill', SUCCESS_COLOR);
          this.canvas.renderAll();
        } else if (newState === STATE.FAILED) {
          stage.set('fill', DANGER_COLOR);
          this.canvas.renderAll();
        }
      });
    },

    /**
     * Setup canvas event handler. (e.g. selection event)
     */
    setUpCanvasEventHandler() {
      this.canvas.on('selection:created', options => {
        this.objectSelected = true;
        this.$eventBus.$emit('metric-select', options.target.metricId);
      });

      this.canvas.on('selection:updated', options => {
        this.objectSelected = true;
        this.$eventBus.$emit('metric-select', options.target.metricId);
      });

      this.canvas.on('selection:cleared', () => {
        this.objectSelected = false;
        this.$eventBus.$emit('metric-deselect');
      });

      this.canvas.on('mouse:over', options => {
        if (options.target && !this.objectSelected) {
          this.$eventBus.$emit('metric-select', options.target.metricId);
        } else if (!options.target && !this.objectSelected) {
          this.$eventBus.$emit('metric-deselect');
        }
      })
    },

    /**
     * Reset all coordinates of each object inside of canvas.
     * This method should be called when inner coordinate system changed.
     */
    resetCoords() {
      this.canvas.forEachObject(obj => {
        obj.setCoords();
      });
    },

    /**
     * Set zoom and viewport of canvas to DAG fit in canvas size.
     * This method is asynchronous.
     */
    async fitCanvas() {
      let widthRatio = this.canvas.width / (this.dagWidth);
      let heightRatio = this.canvas.height / (this.dagHeight);
      let targetRatio = widthRatio < heightRatio ?
        heightRatio : widthRatio;
      this.canvas.viewportTransform[4] = -this.dagOriginX * targetRatio;
      this.canvas.viewportTransform[5] = -this.dagOriginY * targetRatio;
      this.canvas.setZoom(targetRatio);
      this.rearrangeFontSize(targetRatio);
      this.canvas.renderAll();
    },

    /**
     * Rearrange font size according to the canvas zoom ratio.
     */
    rearrangeFontSize(ratio) {
      if (this.stageIdArray) {
        this.stageIdArray.forEach(stageId => {
          if (this.stageInnerTextObjects[stageId]) {
            this.stageInnerTextObjects[stageId].forEach(text => {
              text.set('fontSize', FONT_SIZE * ratio);
            });
          }
        });
      }

      this.stageEdgeTextObjects.forEach(text => {
        text.set('fontSize', FONT_SIZE * ratio);
      });

      this.stageTextObjects.forEach(text => {
        text.set('fontSize', FONT_SIZE * ratio);
      });
    },

    /**
     * Fetch inner irDag of DAG object.
     * Should be modified when DAG object structure is changed in Nemo side.
     */
    getInnerIrDag(stageId) {
      return this.dag.vertices.find(v => v.id === stageId).properties.irDag;
    },

    /**
     * Set stage states. Should be called once when job id is changed,
     * and redrawing new DAG.
     * @param states stage id to state map.
     */
    setStates(states) {
      Object.keys(states).forEach(stageId => {
        const stage = this.stageObjects[stageId];
        const state = states[stageId];

        if (stage && state) {
          if (state === STATE.COMPLETE) {
            stage.set('fill', SUCCESS_COLOR);
            this.canvas.renderAll();
          } else if (state === STATE.FAILED) {
            stage.set('fill', DANGER_COLOR);
            this.canvas.renderAll();
          }
        }
      });
    },

    /**
     * Draw DAG.
     * This method decides each object's coordinates and
     * overall DAG shape(topology).
     */
    drawDAG() {
      // clear objects in canvas without resetting background
      this.canvas.remove(...this.canvas.getObjects().concat());

      let objectArray = [];
      // configure stage layout based on inner vertices
      this.stageIdArray.forEach(stageId => {
        const irDag = this.getInnerIrDag(stageId);
        const innerEdges = irDag.edges;
        const innerVertices = irDag.vertices;

        // initialize stage inner object array
        this.stageInnerObjects[stageId] = [];
        this.stageInnerTextObjects[stageId] = [];

        // get inner vertex layout
        this.verticesGraph[stageId] = new Graph();
        let g = this.verticesGraph[stageId];

        g.setGraph({});
        g.setDefaultEdgeLabel(function () { return {}; });

        innerVertices.forEach(vertex => {
          g.setNode(vertex.id, {
            label: vertex.id,
            width: VERTEX_WIDTH,
            height: VERTEX_HEIGHT,
          });
        });

        innerEdges.forEach(edge => {
          g.setEdge(edge.src, edge.dst, {
            label: edge.properties.runtimeEdgeId,
            width: 10,
            height: 10,
            labelpos: 'c',
          });
        });

        // generate layout
        dagre.layout(g);

        // create vertex circles
        g.nodes().map(node => g.node(node)).forEach(vertex => {
          let vertexCircle = new fabric.Circle({
            metricId: vertex.label,
            radius: VERTEX_RADIUS,
            left: vertex.x,
            top: vertex.y,
            originX: 'center',
            originY: 'center',
            fill: 'black',
            hasControls: false,
            hasRotatingPoint: false,
            lockMovementX: true,
            lockMovementY: true,
          });

          this.vertexObjects[vertex.label] = vertexCircle;
          this.stageInnerObjects[stageId].push(vertexCircle);
          objectArray.push(vertexCircle);
        });

        g.edges().map(e => g.edge(e)).forEach(edge => {
          let path = this.drawSVGEdgeWithArrow(edge);
          let edgeLabelObj = new fabric.Text(edge.label, {
            left: edge.x,
            top: edge.y,
            fontSize: FONT_SIZE,
            originX: 'center',
            originY: 'center',
            selectable: false,
          });
          objectArray.push(edgeLabelObj);
          this.stageInnerTextObjects[stageId].push(edgeLabelObj);

          let pathObj = new fabric.Path(path);
          pathObj.set({
            metricId: edge.label,
            fill: 'transparent',
            stroke: 'black',
            strokeWidth: 2,
            perPixelTargetFind: true,
            hasControls: false,
            hasRotatingPoint: false,
            lockMovementX: true,
            lockMovementY: true,
          });

          objectArray.push(pathObj);
          this.stageInnerObjects[stageId].push(pathObj);
        });

      });

      this.stageGraph = new Graph();
      let g = this.stageGraph;

      g.setGraph({ rankdir: 'TB' });
      g.setDefaultEdgeLabel(function () { return {}; });

      this.stageIdArray.forEach(stageId => {
        const vg = this.verticesGraph[stageId];
        g.setNode(stageId, {
          label: stageId,
          width: vg.graph().width,
          height: vg.graph().height,
        });
      });

      this.stageEdges.forEach(stageEdge => {
        g.setEdge(stageEdge.src, stageEdge.dst, {
          label: stageEdge.properties.runtimeEdgeId,
          width: 10,
          height: 10,
          labelpos: 'c',
        });
      });

      dagre.layout(g);

      // create stage rect
      g.nodes().map(node => g.node(node)).forEach(stage => {
        let stageRect = new fabric.Rect({
          metricId: stage.label,
          width: stage.width,
          height: stage.height,
          left: stage.x,
          top: stage.y,
          rx: RECT_ROUND_RADIUS,
          ry: RECT_ROUND_RADIUS,
          fill: 'white',
          stroke: 'rgba(100, 200, 200, 0.5)',
          strokeWidth: RECT_STROKE_WIDTH,
          originX: 'center',
          originY: 'center',
          hasControls: false,
          hasRotatingPoint: false,
          lockMovementX: true,
          lockMovementY: true,
        });

        let stageLabelObj = new fabric.Text(stage.label, {
          left: stage.x,
          top: stage.y - stage.height / 2 + FONT_SIZE * 2,
          fontSize: FONT_SIZE,
          originX: 'center',
          originY: 'center',
          selectable: false,
        });
        this.stageTextObjects.push(stageLabelObj);

        this.stageObjects[stage.label] = stageRect;
        this.canvas.add(stageRect);
        this.canvas.add(stageLabelObj);
        stageRect.sendToBack();
        stageLabelObj.bringToFront();
      });

      let stageEdgeObjectArray = [];
      g.edges().map(e => g.edge(e)).forEach(edge => {
        let path = this.drawSVGEdgeWithArrow(edge);
        let edgeLabelObj = new fabric.Text(edge.label, {
          left: edge.x,
          top: edge.y,
          fontSize: FONT_SIZE,
          originX: 'center',
          originY: 'center',
          selectable: false,
        });
        stageEdgeObjectArray.push(edgeLabelObj);
        this.stageEdgeTextObjects.push(edgeLabelObj);

        let pathObj = new fabric.Path(path);
        pathObj.set({
          metricId: edge.label,
          fill: 'transparent',
          stroke: 'black',
          strokeWidth: EDGE_STROKE_WIDTH,
          perPixelTargetFind: true,
          hasControls: false,
          hasRotatingPoint: false,
          lockMovementX: true,
          lockMovementY: true,
        });

        stageEdgeObjectArray.push(pathObj);
      });

      stageEdgeObjectArray.forEach(e => {
        this.canvas.add(e);
      });

      // rearrange inner vertices and edges
      this.stageIdArray.forEach(stageId => {
        const stageObj = this.stageObjects[stageId];
        this.stageInnerObjects[stageId].forEach(obj => {
          const dx = obj.get('left') + stageObj.get('left')
            - stageObj.get('width') / 2;
          const dy = obj.get('top') + stageObj.get('top')
            - stageObj.get('height') / 2;
          obj.set('left', dx);
          obj.set('top', dy);
        });

        this.stageInnerTextObjects[stageId].forEach(obj => {
          const dx = obj.get('left') + stageObj.get('left')
            - stageObj.get('width') / 2;
          const dy = obj.get('top') + stageObj.get('top')
            - stageObj.get('height') / 2;
          obj.set('left', dx);
          obj.set('top', dy);
        })
      });

      objectArray.forEach(obj => {
        this.canvas.add(obj);
      });
    },

    /**
     * Build SVG path with arrow head.
     * @param edge object to draw.
     */
    drawSVGEdgeWithArrow(edges) {
      let path = '';
      edges.points.forEach(point => {
        if (!path) {
          path = `M ${point.x} ${point.y}`;
        } else {
          path += ` L ${point.x} ${point.y}`;
        }
      });

      const l = edges.points.length,
        a = ARROW_SIDE, h = a * Math.sqrt(3) / 2;
      const p1 = edges.points[l - 2], p2 = edges.points[l - 1];
      let theta = Math.atan2(p2.y - p1.y, p2.x - p1.x);

      let trans = (d, theta) => {
        let c = Math.cos(theta), s = Math.sin(theta);
        return { x: c * d.x - s * d.y, y: s * d.x + c * d.y };
      };

      let d = [
        { x: -h, y: 0 },
        { x: 0, y: a / 2 },
        { x: h, y: -a / 2 },
        { x: -h, y: -a / 2 },
        { x: 0, y: a / 2 }
      ].map(_d => trans(_d, theta));

      d.forEach(p => {
        path += ` l ${p.x} ${p.y}`;
      });

      return path;
    },
  }
}
</script>
<style>
.dag-canvas {
  box-sizing: border-box;
}

.fit-button {
  margin: 10px;
}
</style>
