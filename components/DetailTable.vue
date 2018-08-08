<template>
  <div>
    <el-table
      border
      empty-text="No data"
      :row-class-name="_rowClassName"
      :data="tableData">
      <el-table-column type="expand">
        <template slot-scope="props">
          <ul>
            <li v-for="ep in props.row.extra" :key="ep.key">
              {{ _preprocessKey(ep.key) }}: {{ ep.value }}
            </li>
          </ul>
        </template>
      </el-table-column>
      <el-table-column label="Key" prop="key"/>
      <el-table-column label="Value" prop="value"/>
    </el-table>
  </div>
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

    _preprocessKey(key) {
      if (/^.+\..+$/.test(key)) {
        const splitted = key.split('.');
        return splitted[splitted.length - 1];
      }
      return key;
    },
  },
}
</script>
<style>
.no-expand .el-icon {
  display: none;
}

.no-expand .el-table__expand-icon {
  pointer-events: none;
}
</style>
