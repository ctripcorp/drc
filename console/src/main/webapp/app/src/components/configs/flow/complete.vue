<template>
      <div style="text-align:center">
        <h3>已完成该表同步的高级配置！</h3>
      </div>
</template>
<script>
export default {
  name: 'complete',
  props: {
    srcMha: String,
    destMha: String,
    applierGroupId: Number,
    srcDc: String,
    destDc: String,
    dataMediaId: Number,
    namespace: String,
    name: String,
    tableData: Array
  },
  data () {
    return {
    }
  }
}
</script>
<style scoped>
</style>
