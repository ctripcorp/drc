<template>
      <div style="text-align:center">
        <h3 >行过滤配置暂未迁移，请使用老方式！</h3>
      </div>
</template>

<script>
export default {
  name: 'rowsFilterConfig',
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
  },
  methods: {
    handleSubmit () {
      // todo
    },
    submitConfig () {
      // todo
    }
  }
}
</script>

<style scoped>

</style>
