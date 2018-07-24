<template>
  <el-card>
    <el-table
      border
      empty-text="No data"
      :row-class-name="_rowClassName"
      :data="tableData">
      <el-table-column type="expand">
        <template slot-scope="props">
          <ul>
            <li v-for="ep in props.row.extra" :key="ep.key">
              {{ ep.key }}: {{ ep.value }}
            </li>
          </ul>
        </template>
      </el-table-column>
      <el-table-column label="Key" prop="key"/>
      <el-table-column label="Value" prop="value"/>
    </el-table>
  </el-card>
</template>
<script>
export default {
  props: ['tableData'],

  methods: {
    _rowClassName(rowObject) {
      if (rowObject.row.extra) {
        return '';
      }
      return 'no-expand';
    },
  },

  watch: {
    tableData: {
      handler() {
        console.log('changed');
      },
      deep: true
    }
  },
}
</script>
