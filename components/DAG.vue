<template>
  <div ref="canvasContainer" class="dag-canvas-container">
    <p v-if="!dag">DAG is not ready.</p>
    <canvas :class="{dag: dag-canvas}" id="dag-canvas"></canvas>
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

const SUCCESS_COLOR = '#67C23A';

const CANVAS_RATIO = 0.75;
const MAX_ZOOM = 20;
const MIN_ZOOM = 0.01;
const TARGET_FIND_TOLERANCE = 4;

const DEBUG = false;

export default {

  props: ['metricDataSet', 'metricLookupMap'],

  mounted() {
    this.initializeCanvas();
    this.setUpEventListener();

    // debug
    if (DEBUG) {
      this.dag = require('~/assets/sample.json').dag;
      this.resizeCanvas(false);
      this.drawDAG();
      this.fitCanvas();
      this.setUpCanvasMouseEventHandler();
    }
    // debug end
  },

  data() {
    return {
      canvas: undefined,
      isDragging: false,
      lastXCoord: 0,
      lastYCoord: 0,
      resizeDebounceTimer: undefined,
      lastViewportX: 0,
      lastViewportY: 0,

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
      this.canvas = new fabric.Canvas('dag-canvas');
      this.canvas.selection = false;
      this.canvas.targetFindTolerance = TARGET_FIND_TOLERANCE;
    },

    resizeCanvas(fit) {
      if (!this.canvas) {
        return;
      }

      if (this.resizeDebounceTimer) {
        clearTimeout(this.resizeDebounceTimer);
      }

      this.resizeDebounceTimer = setTimeout(() => {
        let w = this.$refs.canvasContainer.offsetWidth;
        this.canvas.setWidth(w);
        this.canvas.setHeight(w * CANVAS_RATIO);
        if (fit) {
          this.fitCanvas();
        }
      }, DEBOUNCE_INTERVAL);
    },

    setUpEventListener() {
      if (process.browser) {
        window.addEventListener('resize',
          () => this.resizeCanvas(true), false);
      }

      this.$eventBus.$on('resize-canvas', () => {
        this.resizeCanvas(true);
      });

      this.$eventBus.$on('rerender-dag', () => {
        if (this.firstDagRender) {
          this.firstDagRender = false;
          this.resizeCanvas(true);
        } else {
          this.resizeCanvas(false);
        }
        this.canvas.viewportTransform[4] = this.lastViewportX;
        this.canvas.viewportTransform[5] = this.lastViewportY;
        this.canvas.calcOffset();
      });

      // new dag event
      this.$eventBus.$on('dag', data => {
        this.setUpCanvasMouseEventHandler();
        this.dag = data;
        this.drawDAG();
        this.fitCanvas();
        this.canvas.viewportTransform[4] = this.lastViewportX;
        this.canvas.viewportTransform[5] = this.lastViewportY;
        this.firstDagRender = true;
      });

      this.$eventBus.$on('metric-selected-done', () => {
        this.resizeCanvas(true);
        this.canvas.renderAll();
        this.canvas.calcOffset();
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

    setUpCanvasMouseEventHandler() {
      // zoom feature
      this.canvas.on('mouse:wheel', option => {
        let delta = option.e.deltaY;
        let zoom = this.canvas.getZoom();
        zoom += delta / 200;
        if (zoom > MAX_ZOOM) {
          zoom = MAX_ZOOM;
        } else if (zoom < MIN_ZOOM) {
          zoom = MIN_ZOOM;
        }
        this.canvas.zoomToPoint({
          x: option.e.offsetX,
          y: option.e.offsetY,
        }, zoom);
        this.lastViewportX = this.canvas.viewportTransform[4];
        this.lastViewportY = this.canvas.viewportTransform[5];
        option.e.preventDefault();
        option.e.stopPropagation();
      });

      // pan feature
      this.canvas.on('mouse:down', option => {
        let e = option.e;
        this.isDragging = true;
        this.lastXCoord = e.clientX;
        this.lastYCoord = e.clientY;

        if (!option.target) {
          this.$eventBus.$emit('metric-deselect');
        }
      });

      this.canvas.on('mouse:move', option => {
        if (this.isDragging) {
          const e = option.e;
          this.canvas.viewportTransform[4] += e.clientX - this.lastXCoord;
          this.canvas.viewportTransform[5] += e.clientY - this.lastYCoord;
          this.lastViewportX = this.canvas.viewportTransform[4];
          this.lastViewportY = this.canvas.viewportTransform[5];
          this.canvas.requestRenderAll();
          this.lastXCoord = e.clientX;
          this.lastYCoord = e.clientY;
        }
      });

      this.canvas.on('mouse:up', option => {
        this.isDragging = false;
        this.resetCoords();
      });
    },

    resetCoords() {
      this.canvas.forEachObject(obj => {
        obj.setCoords();
      });
    },

    fitCanvas() {
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

    handleObjectClick({ metricId }) {
      this.$eventBus.$emit('metric-selected', metricId);
    },

    drawDAG() {
      this.canvas.clear();

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
            selectable: false,
          });

          vertexCircle.on('mousedown', () => {
            this.handleObjectClick(vertexCircle);
          });

          this.vertexObjects[vertex.label] = vertexCircle;
          this.stageInnerObjects[stageId].push(vertexCircle);
          objectArray.push(vertexCircle);
        });

        g.edges().map(e => g.edge(e)).forEach(edge => {
          let path = '';
          edge.points.forEach(point => {
            if (!path) {
              path = `M ${point.x} ${point.y}`;
            } else {
              path += ` L ${point.x} ${point.y}`;
            }
          });

          let pathObj = new fabric.Path(path);
          pathObj.set({
            metricId: edge.label,
            fill: 'transparent',
            stroke: 'black',
            strokeWidth: 2,
            perPixelTargetFind: true,
            selectable: false,
          });

          pathObj.on('mousedown', () => {
            this.handleObjectClick(pathObj);
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
          selectable: false,
        });

        this.stageObjects[stage.label] = stageRect;
        this.canvas.add(stageRect);
        stageRect.sendToBack();

        stageRect.on('mousedown', () => {
          this.handleObjectClick(stageRect);
        });
      });

      let stageEdgeObjectArray = [];
      g.edges().map(e => g.edge(e)).forEach(edge => {
        let path = '';
        edge.points.forEach(point => {
          if (!path) {
            path = `M ${point.x} ${point.y}`;
          } else {
            path += ` L ${point.x} ${point.y}`;
          }
        });

        let pathObj = new fabric.Path(path);
        pathObj.set({
          metricId: edge.label,
          fill: 'transparent',
          stroke: 'black',
          strokeWidth: 2,
          perPixelTargetFind: true,
          selectable: false,
        });

        pathObj.on('mousedown', () => {
          this.handleObjectClick(pathObj);
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
  }
}
</script>
<style>
.dag-canvas {
  box-sizing: border-box;
  border: 2px solid black;
}
</style>
