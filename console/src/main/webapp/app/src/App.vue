<template>
  <div id="app">
    <router-view></router-view>
    <remote-script src="//webresint.ctripcorp.com/ares2/infosec/ifs/*/default/lab.min.js?expires=1s"></remote-script>
  </div>
</template>

<script>
import '../src/common/remote-script.js'
export default {

}
</script>

<style>
  body{
    height: auto;
    background: #f8f8f9 !important;
  }
  #fontsize .ivu-form-item-label{
    font-size: 15px;
    color: red;
  }
</style>
