<template>
  <el-card>
    <el-upload
      drag
      :show-file-list="false">
      <i class="el-icon-upload"/>
      <div class="el-upload__text">
        Drop JSON here or <em>click here to upload.</em>
      </div>
    </el-upload>
  </el-card>
</template>

<script>
export default {
  // TODO: manage multiple WebSocket object
  // and also multiple endpoints
  // TODO: upload json dump file
}
</script>
