<template>
  <div>
    <canvas class="dag-canvas" id="dag-canvas"></canvas>
    <button @click="drawDAG()">DRAW</button>
  </div>
</template>

<script>
import { fabric } from 'fabric';
import { Graph } from '@dagrejs/graphlib';
import graphlib from '@dagrejs/graphlib';
import dagre from 'dagre';

const VERTEX_WIDTH = 50;
const VERTEX_HEIGHT = 30;
const VERTEX_RADIUS = 4;
const PAN_MARGIN = 20;

export default {

  mounted() {
    this.canvas = new fabric.Canvas('dag-canvas');
    this.canvas.setWidth(800);
    this.canvas.setHeight(300);
    this.canvas.selection = false;
    this.$eventBus.$on('dag', data => {
      this.dag = data;
      this.drawDAG();
    });

    // FOR DEBUG
    this.dag = require('~/assets/monster.json').dag;
    this.drawDAG();
    // FOR DEBUG END

    // zoom feature
    this.canvas.on('mouse:wheel', option => {
      let delta = option.e.deltaY;
      let zoom = this.canvas.getZoom();
      zoom += delta / 200;
      if (zoom > 20) {
        zoom = 20;
      } else if (zoom < 0.01) {
        zoom = 0.01;
      }
      this.canvas.zoomToPoint({
        x: option.e.offsetX,
        y: option.e.offsetY,
      }, zoom);
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
        this.canvas.requestRenderAll();
        this.lastXCoord = e.clientX;
        this.lastYCoord = e.clientY;
      }
    });

    this.canvas.on('mouse:up', option => {
      this.isDragging = false;
    });
  },

  data() {
    return {
      canvas: undefined,
      isDragging: false,
      lastXCoord: 0,
      lastYCoord: 0,

      dag: undefined,

      stageGraph: undefined,
      verticesGraph: {},

      // fabric.Group of stages
      stageGroups: {},

      // object vertexId -> fabric.Circle of vertex
      vertices: {},
    };
  },

  computed: {
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
        const vertexCount = innerVertices.length;
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
