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
const VERTEX_RADIUS = 4;
const PAN_MARGIN = 20;

const CANVAS_RATIO = 0.75;
const MAX_ZOOM = 20;
const MIN_ZOOM = 0.01;

const DEBUG = false;

export default {

  mounted() {
    this.initializeCanvas();
    this.setUpEventListener();

    // debug
    if (DEBUG) {
      this.dag = require('~/assets/monster.json').dag;
      this.resizeCanvas(false);
      this.drawDAG();
      this.fitCanvas();
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

      // fabric.Group of stages
      stageGroups: {},

      // object vertexId -> fabric.Circle of vertex
      vertices: {},
      // object stageId -> fabric.Rect of stage
      stages: {},
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

      this.$eventBus.$on('rerender-dag', () => {
        if (this.firstDagRender) {
          this.firstDagRender = false;
          this.resizeCanvas(true);
        } else {
          this.resizeCanvas(false);
        }
        this.canvas.viewportTransform[4] = this.lastViewportX;
        this.canvas.viewportTransform[5] = this.lastViewportY;
        //this.canvas.calcOffset();
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

      // stage state transition event
      this.$eventBus.$on('stageEvent', ({ stageId, state }) => {
        if (!stageId || !(stageId in this.stages)) {
          return;
        }

        const stage = this.stages[stageId];

        if (state == STATE.COMPLETE) {
          stage.set('fill', 'green');
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
      return this.dag.vertices.find(v => v.id == stageId).properties.irDag;
    },

    drawDAG() {
      this.canvas.clear();
      // configuring stage layout based on inner vertices
      this.stageIdArray.forEach((stageId, idx) => {
        const irDag = this.getInnerIrDag(stageId);
        const innerEdges = irDag.edges;
        const innerVertices = irDag.vertices;
        let objectArray = [];

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
            radius: VERTEX_RADIUS,
            left: vertex.x,
            top: vertex.y,
            originX: 'center',
            originY: 'center',
            fill: 'black',
          });

          this.vertices[vertex.label] = vertexCircle;
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
            fill: 'transparent',
            stroke: 'black',
            strokeWidth: 2,
          });
          objectArray.push(pathObj);
        });

        this.stageGroups[stageId] = new fabric.Group(objectArray, {
          selectable: false,
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
          width: stage.width,
          height: stage.height,
          fill: 'white',
          stroke: 'rgba(100, 200, 200, 0.5)',
          strokeWidth: 2,
        });

        this.stages[stage.label] = stageRect;

        const sg = this.stageGroups[stage.label];
        // add rect to group
        sg.addWithUpdate(stageRect);
        stageRect.sendToBack();
        // set group attributes
        sg.set('left', stage.x);
        sg.set('top', stage.y);
        sg.set('originX', 'center');
        sg.set('originY', 'center');
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
          fill: 'transparent',
          stroke: 'black',
          strokeWidth: 2,
          selectable: false,
        });

        stageEdgeObjectArray.push(pathObj);
      });

      this.canvas.add(new fabric.Group(stageEdgeObjectArray, {
        selectable: false,
      }));

      // draw final result by adding groups to canvas
      this.stageIdArray.forEach(stageId => {
        this.canvas.add(this.stageGroups[stageId]);
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
