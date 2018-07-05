<template>
  <el-container>
    <el-header>
      <div class="header-container">
        <h2 class="header-title">Nemo</h2>
      </div>
    </el-header>
    <el-main>
      <div class="main-container">
        <nuxt/>
      </div>
    </el-main>
  </el-container>
</template>

<style>
html {
  -ms-text-size-adjust: 100%;
  -webkit-text-size-adjust: 100%;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  box-sizing: border-box;
}

*, *:before, *:after {
  box-sizing: border-box;
  margin: 0;
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
}

.header-container {
  height: 80px;
  max-width: 1200px;
  margin-left: auto;
  margin-right: auto;
  margin-top: 30px;
}

.main-container {
  max-width: 1200px;
  margin-left: auto;
  margin-right: auto;
}

.header-title {
  font-size: 32px;
}
</style>
