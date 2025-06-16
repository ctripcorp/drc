<template>
  <base-component>
    <iframe :loading="dataLoading" :src="panelUrl"  :style="{padding: '10px', marginLeft: '185px', marginRight: '0px', border: 'none' }"  height="1700px">
    </iframe>
  </base-component>
</template>

<script>
export default {
  data () {
    return {
      dataLoading: false,
      panelUrl: ''
    }
  },
  created () {
    const that = this
    this.dataLoading = true
    this.axios.get('/api/drc/v1/meta/panelUrl').then((response) => {
      const currentProtocol = window.location.protocol
      const url = response.data.data.replace(/^https?:/, currentProtocol) // 去掉冒号
      that.panelUrl = url
      that.dataLoading = false
      console.log(that.panelUrl)
    })
  }
}
</script>

<style scoped>

</style>
