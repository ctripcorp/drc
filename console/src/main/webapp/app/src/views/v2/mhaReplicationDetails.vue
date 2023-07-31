<template>
  <div>
    <div style="height:calc(100vh - 50px);">
      <RelationGraph
        ref="seeksRelationGraph"
        :options="graphOptions"
        :on-node-click="onNodeClick"
        :on-line-click="onLineClick"
      />
    </div>
  </div>
</template>

<script>
export default {
  name: 'mhaReplicationVisualization',
  components: {},
  data () {
    return {
      dataLoading: false,
      graphData: {
        graphInit: false,
        mhaIdRequested: new Set(),
        mhaIdShown: new Set(),
        replicationIdShown: new Set(),
        rootId: '',
        nodes: [
          // { id: '3365', text: '节点-1', myicon: 'el-icon-star-on' },
          // { id: '2', text: '节点-2', myicon: 'el-icon-setting' }
        ],
        lines: [
          // { from: '3365', to: '2', text: '投资 双向' },
          // { from: '2', to: '3365', text: '投资 双向' }
        ],
        clear: function () {
          this.nodes = []
          this.lines = []
        },
        isEmpty: function () {
          return this.nodes == null || this.lines == null || this.nodes.length === 0 || this.lines.length === 0
        },
        appendNode: function (mha) {
          if (!this.mhaIdShown.has(mha.id + '')) {
            this.mhaIdShown.add(mha.id + '')
            this.nodes.push({
              id: mha.id + '',
              text: mha.name
              // nodeShape: 1
            })
          }
        },
        appendLine: function (replication) {
          if (!this.replicationIdShown.has(replication.replicationId + '')) {
            this.replicationIdShown.add(replication.replicationId + '')
            this.lines.push({
              from: replication.srcMha.id + '',
              to: replication.dstMha.id + '',
              color: 'rgb(135, 135, 135)',
              selected: true,
              lineWidth: 1
            })
          }
        },
        appendDataToGraph (graph) {
          // eslint-disable-next-line camelcase
          if (!this.graphInit) {
            this.graphInit = true
            graph.setJsonData(this, (graphInstance) => {
              // 这些写上当图谱初始化完成后需要执行的代码
              console.log(graphInstance.getLinks())
              graphInstance.getLinks().forEach(e => {
                console.log(e)
                e.relations.forEach(line => {
                  console.log(line.id)
                  graphInstance.setCheckedLine(line.id)
                })
              })
              graphInstance.dataUpdated()
              console.log('init graph success')
            })
          } else {
            graph.appendJsonData(this, (graphInstance) => {
              // 更新图
              console.log('refresh graph success')
            })
          }
        },
        hasShow (mhaId) {
          if (this.mhaIdRequested.has(mhaId)) {
            return true
          } else {
            this.mhaIdRequested.add(mhaId)
            return false
          }
        }
      },
      graphOptions: {
        // line.color = '#ff0000'
        // line.fontColor = '#ff0000'
        // line.lineWidth = 3
        defaultLineColor: '#333333',
        defaultLineWidth: 2,
        defaultNodeBorderWidth: 1,
        defaultNodeBorderColor: 'rgb(152,213,244)',
        defaultNodeColor: 'rgba(238, 178, 94, 1)',
        allowSwitchLineShape: true,
        allowSwitchJunctionPoint: true,
        checkedLineColor: 'rgba(30, 144, 255, 1)', // 当线条被选中时的颜色
        defaultLineShape: 1,
        layouts: [
          {
            label: '自动布局',
            layoutName: 'force',
            layoutClassName: 'seeks-layout-force'
          }
        ],
        defaultJunctionPoint: 'border'
        // 这里可以参考"Graph 图谱"中的参数进行设置
      }
    }
  },
  mounted () {
    const mhaId = this.$route.query.mhaId
    console.log('mhaId' + mhaId)
    this.showMore(mhaId)
  },
  methods: {
    async showMore (mhaId, nodeObject) {
      if (this.graphData.hasShow(mhaId)) {
        return
      }
      const reqParam = {
        relatedMhaId: mhaId
      }
      await this.axios.get('/api/drc/v2/replication/queryMhaRelated', { params: reqParam })
        .then(response => {
          const replications = response.data.data
          const emptyResult = replications == null || !Array.isArray(replications) || replications.length === 0
          if (emptyResult) {
            this.$Message.error('结果为空')
            return
          }
          // append graph data
          this.appendGraphData(replications)
          if (this.graphData.isEmpty()) {
            this.$Message.success('无新数据')
            return
          }
          // refresh graph
          this.graphData.appendDataToGraph(this.$refs.seeksRelationGraph)
          this.$Message.success('成功')
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    onNodeClick (nodeObject, $event) {
      console.log('onNodeClick:', nodeObject.id)
      this.dataLoading = true
      this.showMore(nodeObject.id)
    },
    onLineClick (lineObject, linkObject, $event) {
      console.log('onLineClick:', lineObject)
    },
    appendGraphData: function (replications) {
      this.graphData.clear()
      replications.forEach((replication) => {
        this.graphData.appendNode(replication.srcMha)
        this.graphData.appendNode(replication.dstMha)
        this.graphData.appendLine(replication)
      })
    }
  }
}
</script>

<style lang="scss">
</style>

<style lang="scss" scoped>

</style>
