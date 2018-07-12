<template>
  <div>
    <svg id="dag-canvas"></svg>
    <button @click="renderGraph()">Render</button>
  </div>
</template>

<script>
import * as d3 from 'd3';
import dagreD3 from 'dagre-d3';

export default {
  mounted() {
    this.svg = d3.select('#dag-canvas');
    this.svgGroup = this.svg.append('g');
    this.render = new dagreD3.render();
    this.graph = new dagreD3.graphlib.Graph({ compound: true, multigraph: true })
      .setGraph({});
  },

  data() {
    return {
      svg: undefined,
      svgGroup: undefined,
      render: undefined,
      graph: undefined,
    };
  },

  methods: {
    renderGraph() {
      let g = this.graph;
      g.setNode('a', { label: 'A' });
      g.setNode('b', { label: 'B' });
      g.setNode('c', { label: 'C' });
      // g.setNode('group', { label: 'Stage', clusterLabelPos: 'top', style: 'fill: d3d7e8' });

      // g.setParent('group', 'b');

      g.setEdge('a', 'b', { label: 'ab' });
      g.setEdge('b', 'c', { label: 'bc' });

      this.render(d3.select('svg#dag-canvas g'), g);
    },
  }
}
</script>
