<template>
  <div style="margin-top: 100px">
    <Card style="margin-left:250px; width:650px">
      <div style="text-align:center">
        <h3>表映射配置暂未迁移，请使用老方式！</h3>
      </div>
    </Card>
  </div>
</template>

<script>

export default {
  name: 'tableMappingConfig',
  props: {
    srcMha: String,
    destMha: String,
    applierGroupId: Number,
    srcDc: String,
    destDc: String,
    dataMediaId: Number,
    namespace: String,
    name: String
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
