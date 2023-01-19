<template>
      <div style="text-align:center">
        <h3>暂不支持字段映射！</h3>
      </div>
</template>

<script>
export default {
  name: 'columnsMappingConfig',
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
