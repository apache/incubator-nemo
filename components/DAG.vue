<template>
  <div ref="canvasContainer" class="dag-canvas-container">
    <el-button
      v-if="dag"
      class="fit-button"
      @click="fitButton"
      plain
      type="primary">Fit</el-button>
    <p v-if="!dag">DAG is not ready.</p>
    <div v-show="dag">
      <canvas class="dag-canvas" id="dag-canvas"></canvas>
    </div>
  </div>
</template>

<script>
import { fabric } from 'fabric';
import { Graph } from '@dagrejs/graphlib';
import graphlib from '@dagrejs/graphlib';
import dagre from 'dagre';
import { STATE } from '~/assets/constants';

const DEBOUNCE_INTERVAL = 200;

const VERTEX_WIDTH = 50;
const VERTEX_HEIGHT = 30;
const VERTEX_RADIUS = 6;
const PAN_MARGIN = 20;
const RECT_ROUND_RADIUS = 4;
const ARROW_SIDE = 3;

const SUCCESS_COLOR = '#67C23A';

const BACKGROUND_COLOR = '#F2F6FC';
const CANVAS_RATIO = 0.75;
const MAX_ZOOM = 20;
const MIN_ZOOM = 0.1;
const TARGET_FIND_TOLERANCE = 4;

const MY_TAB_INDEX = '2';

const DEBUG = false;

export default {

  props: ['metricDataSet', 'tabIndex'],

  mounted() {
    this.initializeCanvas();
    this.setUpEventListener();

    // debug
    if (DEBUG) {
      this.dag = require('~/assets/sample.json').dag;
      this.resizeCanvas(false);
      this.drawDAG();
      this.fitCanvas();
      this.setUpCanvasEventHandler();
    }
    // debug end
  },

  data() {
    return {
      canvas: undefined,
      isDragging: false,
      lastXCoord: 0,
      lastYCoord: 0,
      lastViewportX: 0,
      lastViewportY: 0,

      resizeDebounceTimer: undefined,

      dag: undefined,
      firstDagRender: false,

      stageGraph: undefined,
      verticesGraph: {},

      // vertexId -> fabric.Circle of vertex
      vertexObjects: {},
      // stageId -> fabric.Rect of stage
      stageObjects: {},
      // stageId -> inner objects of stage (edges, vertices)
      stageInnerObjects: {},
    };
  },

  computed: {

    dagWidth() {
      if (!this.stageGraph) {
        return undefined;
      }
      return this.stageGraph.graph().width;
    },

    dagHeight() {
      if (!this.stageGraph) {
        return undefined;
      }
      return this.stageGraph.graph().height;
    },

    stageIdArray() {
      if (!this.dag) {
        return undefined;
      }
      return this.dag.vertices.map(v => v.id);
    },

    stageEdges() {
      if (!this.dag) {
        return undefined;
      }
      return this.dag.edges;
    },

  },

  methods: {
    initializeCanvas() {
      this.canvas = new fabric.Canvas('dag-canvas', {
        selection: false,
        backgroundColor: BACKGROUND_COLOR,
        targetFindTolerance: TARGET_FIND_TOLERANCE,
        preserveObjectStacking: true,
      });
      this.canvas.renderAll();
    },

    initializeVariables() {
      this.isDragging = false;
      this.lastXCoord = 0;
      this.lastYCoord = 0;
      this.lastViewportX = 0;
      this.lastViewportY = 0;
      this.resizeDebounceTimer = undefined;
      this.firstDagRender = false;
      this.stageGraph = undefined;
      this.verticesGraph = {};
      this.vertexObjects = {};
      this.stageObjects = {};
      this.stageInnerObjects = {};
    },

    resizeCanvas(fit) {
      return new Promise((resolve, reject) => {
        if (!this.canvas) {
          return;
        }

        if (this.resizeDebounceTimer) {
          clearTimeout(this.resizeDebounceTimer);
        }

        this.resizeDebounceTimer = setTimeout(() => {
          if (this.$refs.canvasContainer) {
            let w = this.$refs.canvasContainer.offsetWidth;
            this.canvas.setWidth(w);
            this.canvas.setHeight(w * CANVAS_RATIO);
            if (fit) {
              this.fitCanvas();
            }
            resolve();
          }
        }, DEBOUNCE_INTERVAL);
      });
    },

    setUpEventListener() {
      if (process.browser) {
        window.addEventListener('resize',
          () => this.resizeCanvas(true), false);
      }

      this.$eventBus.$on('resize-canvas', () => {
        this.resizeCanvas(true);
      });

      this.$eventBus.$on('rerender-dag', async () => {
        if (!this.dag) {
          return;
        }

        if (this.firstDagRender) {
          this.firstDagRender = false;
          await this.resizeCanvas(true);
        } else {
          await this.resizeCanvas(false);
        }
        this.canvas.viewportTransform[4] = this.lastViewportX;
        this.canvas.viewportTransform[5] = this.lastViewportY;
        this.canvas.calcOffset();
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

      // new dag event
      this.$eventBus.$on('dag', async data => {
        if (this.tabIndex === MY_TAB_INDEX) {
          await this.resizeCanvas(false);
        }
        // TODO: do not initialized when the dag is from same job
        this.initializeVariables();
        this.setUpCanvasEventHandler();
        this.dag = data;
        this.drawDAG();
        await this.fitCanvas();
        this.canvas.viewportTransform[4] = this.lastViewportX;
        this.canvas.viewportTransform[5] = this.lastViewportY;
        this.firstDagRender = true;
      });

      // stage state transition event
      this.$eventBus.$on('stage-event', ({ stageId, state }) => {
        if (!stageId || !(stageId in this.stageObjects)) {
          return;
        }

        const stage = this.stageObjects[stageId];

        if (state === STATE.COMPLETE) {
          stage.set('fill', SUCCESS_COLOR);
          this.canvas.renderAll();
        }
      });
    },

    setUpCanvasEventHandler() {
      // zoom feature
      this.canvas.on('mouse:wheel', options => {
        let delta = options.e.deltaY;
        let zoom = this.canvas.getZoom();
        zoom += delta / 200;
        if (zoom > MAX_ZOOM) {
          zoom = MAX_ZOOM;
        } else if (zoom < MIN_ZOOM) {
          zoom = MIN_ZOOM;
        }
        this.canvas.zoomToPoint({
          x: options.e.offsetX,
          y: options.e.offsetY,
        }, zoom);
        this.lastViewportX = this.canvas.viewportTransform[4];
        this.lastViewportY = this.canvas.viewportTransform[5];
        options.e.preventDefault();
        options.e.stopPropagation();
      });

      // pan feature
      this.canvas.on('mouse:down', options => {
        let e = options.e;
        this.isDragging = true;
        this.lastXCoord = e.clientX;
        this.lastYCoord = e.clientY;
      });

      this.canvas.on('mouse:move', options => {
        if (this.isDragging) {
          const e = options.e;
          this.canvas.viewportTransform[4] += e.clientX - this.lastXCoord;
          this.canvas.viewportTransform[5] += e.clientY - this.lastYCoord;
          this.lastViewportX = this.canvas.viewportTransform[4];
          this.lastViewportY = this.canvas.viewportTransform[5];
          this.canvas.requestRenderAll();
          this.lastXCoord = e.clientX;
          this.lastYCoord = e.clientY;
        }
      });

      this.canvas.on('mouse:up', () => {
        this.isDragging = false;
        this.resetCoords();
      });

      this.canvas.on('selection:created', options => {
        this.$eventBus.$emit('metric-select', options.target.metricId);
      });

      this.canvas.on('selection:updated', options => {
        this.$eventBus.$emit('metric-select', options.target.metricId);
      });

      this.canvas.on('selection:cleared', () => {
        this.$eventBus.$emit('metric-deselect');
      });
    },

    resetCoords() {
      this.canvas.forEachObject(obj => {
        obj.setCoords();
      });
    },

    async fitCanvas() {
      let widthRatio = this.canvas.width / this.dagWidth;
      let heightRatio = this.canvas.height / this.dagHeight;
      let targetRatio = widthRatio > heightRatio ?
        heightRatio : widthRatio;
      this.canvas.setZoom(targetRatio);
      this.canvas.renderAll();
    },

    getInnerIrDag(stageId) {
      return this.dag.vertices.find(v => v.id === stageId).properties.irDag;
    },

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
          let path = this.drawSVGArrow(edge);

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

      g.setGraph({ rankdir: 'LR' });
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
          strokeWidth: 2,
          originX: 'center',
          originY: 'center',
          hasControls: false,
          hasRotatingPoint: false,
          lockMovementX: true,
          lockMovementY: true,
        });

        this.stageObjects[stage.label] = stageRect;
        this.canvas.add(stageRect);
        stageRect.sendToBack();
      });

      let stageEdgeObjectArray = [];
      g.edges().map(e => g.edge(e)).forEach(edge => {
        let path = this.drawSVGArrow(edge);

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
        })
      });

      objectArray.forEach(obj => {
        this.canvas.add(obj);
      });
    },

    drawSVGArrow(edges) {
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
        { x: 0, y: a / 2 },
        { x: h, y: -a / 2 },
        { x: -h, y: -a / 2 },
        { x: 0, y: a / 2 }
      ].map(_d => trans(_d, theta));

      d.forEach(p => {
        path += ` l ${p.x} ${p.y}`
      });

      return path;
    },

    fitButton() {
      this.canvas.viewportTransform[4] = 0;
      this.canvas.viewportTransform[5] = 0;
      this.fitCanvas();
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
