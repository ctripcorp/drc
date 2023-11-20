<template>
  <div>

    <Row :gutter=10>
      <Col span="10">
        <Input prefix="ios-search" v-model="queryParam.mhaNames" placeholder="请输入集群名"
               @on-enter="showMore([queryParam.mhaNames], true)">
        </Input>
      </Col>
      <Col span="10">
        <Col span="3">
          <Button type="primary" icon="ios-search" :loading="dataLoading" @click="showMore([queryParam.mhaNames], true)">查询所有</Button>
        </Col>
      </Col>
    </Row>
    <div ref="myPage" style="height:calc(100vh - 35vh);width: calc(100vw - 20vw)" @click="graphData.isShowNodeMenuPanel = false; graphData.isShowNodeTipsPanel = false; graphData.nodeMenuPanelPosition.fixed = false">
      <RelationGraph
        ref="seeksRelationGraph"
        :options="graphOptions"
        :on-node-click="onNodeClick"
        :on-line-click="onLineClick"
      >
        <div slot="node" slot-scope="{node}">
<!--          <div style="color: #ffffff;font-size: 16px;position: absolute;width: auto;height:25px;line-height: 25px;margin-top:5px;margin-left:0px;text-align: center;background-color: rgb(0,80,41);">-->
<!--            {{ node.data.mha.regionName }}-->
<!--          </div>-->
          <Tag  color="success" style="position: absolute;width: auto;height:25px;margin-top:-10px;margin-left:15px;text-align: center;font-size: 15px">{{ node.data.mha.regionName }}</Tag>
          <div style=" font-weight:bold; position: absolute;width: auto;height:auto;left:50%;top:50%;-webkit-transform: translate(-50%, -50%);transform: translate(-50%, -50%);text-align: center;">{{ node.data.mha.name }}</div>
        </div>
      </RelationGraph>
    </div>

    <div v-if="graphData.isShowNodeTipsPanel" :style="{left: graphData.nodeMenuPanelPosition.x + 'px', top: graphData.nodeMenuPanelPosition.y + 'px' }" style="z-index: 999;padding:10px;background-color: #ffffff;border:#eeeeee solid 1px;box-shadow: 0px 0px 8px #cccccc;position: absolute;">
      <div class="c-node-menu-item">名称: {{current.node.mha.name}}</div>
      <div class="c-node-menu-item">部门: {{current.node.mha.buName}}</div>
      <div class="c-node-menu-item">地域: {{current.node.mha.regionName}}</div>
    </div>

    <div v-show="graphData.isShowNodeMenuPanel" :style="{left: graphData.linkMenuPanelPosition.x + 'px', top: graphData.linkMenuPanelPosition.y + 'px' }" style="z-index: 999;padding:10px;background-color: #ffffff;border:#eeeeee solid 1px;box-shadow: 0px 0px 8px #cccccc;position: absolute;">
      <div style="line-height: 25px;padding-left: 10px;color: #888888;font-size: 12px;">{{current.link.srcMha.name +' -> '+current.link.dstMha.name}}</div>
      <div style="line-height: 25px;padding-left: 10px;color: #888888;font-size: 12px;">操作:</div>
      <div id="operationsDiv">
        <div v-for="(item, index) in operations" :key="index">
          <div class="c-node-menu-item" @click.stop="item.method(current.link.srcMha.name, current.link.dstMha.name, current.link.replicationId)">{{ item.text }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import prettyMilliseconds from 'pretty-ms'

export default {
  name: 'MhaGraph',
  components: {},
  props: {
    mhaIdList: Array,
    mhaNameList: Array,
    operations: Array
  },
  data () {
    return {
      dataLoading: false,
      graphData: {
        graphInit: false,
        graph: null,
        currentLink: null,
        mhaNameRequested: new Set(),
        mhaNameShown: new Set(),
        replicationIdShown: new Set(),
        isShowNodeMenuPanel: false,
        isShowNodeTipsPanel: false,
        nodeMenuPanelPosition: { x: 0, y: 0, fixed: false },
        linkMenuPanelPosition: { x: 0, y: 0 },
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
        reset: function () {
          this.graphInit = false
          this.mhaNameRequested.clear()
          this.mhaNameShown.clear()
          this.replicationIdShown.clear()
          this.nodes = []
          this.lines = []
        },
        isEmpty: function () {
          return this.nodes == null || this.lines == null || this.nodes.length === 0 || this.lines.length === 0
        },
        appendNode: function (mha) {
          if (!this.mhaNameShown.has(mha.name + '')) {
            this.mhaNameShown.add(mha.name + '')
            this.nodes.push({
              id: mha.id + '',
              text: mha.name + '\n' + mha.regionName,
              data: {
                mha: mha
              },
              width: 100,
              height: 100,
              styleClass: 'c-g-center'
            })
          }
        },
        appendLine: function (replication) {
          if (!this.replicationIdShown.has(replication.replicationId + '')) {
            this.replicationIdShown.add(replication.replicationId + '')
            this.lines.push({
              from: replication.srcMha.id + '',
              to: replication.dstMha.id + '',
              selected: true,
              color: '#2c2c2c',
              styleClass: 'node-line',
              data: {
                replication: replication
              }
            })
          }
        },
        appendDataToGraph (queryAll) {
          const graph = this.graph
          // eslint-disable-next-line camelcase
          if (!this.graphInit) {
            this.graphInit = true
            graph.setJsonData(this, (graphInstance) => {
              // 这些写上当图谱初始化完成后需要执行的代码
              this.deepenClickedNode(null, queryAll)
              console.log('init graph success')
            })
          } else {
            graph.appendJsonData(this, (graphInstance) => {
              // 更新图
              console.log('refresh graph success')
            })
          }
        },
        hasShow (mhaName) {
          if (this.mhaNameRequested.has(mhaName)) {
            return true
          } else {
            this.mhaNameRequested.add(mhaName)
            return false
          }
        },
        deepenClickedNode (nodeObject, queryAll) {
          const color = 'rgb(0,166,81)'
          if (nodeObject != null) {
            nodeObject.color = color
          } else {
            // refresh all
            const nodes = this.graph.getNodes()
            nodes.forEach(e => {
              if (queryAll || this.mhaNameRequested.has(e.data.mha.name)) {
                e.color = color
              }
            })
          }
        }
      },
      graphOptions: {
        defaultLineColor: '#333333',
        defaultLineWidth: 1,
        defaultNodeBorderWidth: 1,
        defaultNodeShape: 0,
        defaultNodeBorderColor: 'rgb(194,194,194)',
        defaultNodeColor: 'rgba(66,66,66)',
        allowSwitchLineShape: true,
        allowSwitchNodeShape: true,
        checkedLineColor: 'rgba(30, 144, 255, 1)', // 当线条被选中时的颜色
        defaultLineShape: 1,
        moveToCenterWhenRefresh: true,
        useAnimationWhenRefresh: true,
        layouts: [
          {
            label: '自动布局',
            layoutName: 'force',
            layoutClassName: 'seeks-layout-force'
          }
        ],
        defaultJunctionPoint: 'border'
        // 这里可以参考"Graph 图谱"中的参数进行设置
      },
      current: {
        node: {
          mha: {}
        },
        link: {
          srcMha: {},
          dstMha: {},
          replicationId: 0
        }
      },
      queryParam: {
        mhaNames: ''
      }
    }
  },
  mounted () {
    const mhaIdList = this.mhaIdList
    const mhaNameList = this.mhaNameList
    console.log('mount mhaId: ' + mhaIdList)
    this.graphData.graph = this.$refs.seeksRelationGraph
    this.graphData.rootId = mhaIdList[0]
    this.showMore(mhaNameList, false)
  },
  methods: {
    showMore (mhaNameList, queryAll) {
      if (queryAll) {
        this.graphData.reset()
      } else {
        this.graphData.clear()
      }
      const filteredNameList = mhaNameList.filter(mhaName => {
        return !this.graphData.hasShow(mhaName)
      })
      if (filteredNameList == null || filteredNameList.length === 0) {
        return
      }
      const reqParam = {
        relatedMhaNames: filteredNameList.join(','),
        queryAll: queryAll
      }
      this.dataLoading = true
      this.axios.get('/api/drc/v2/replication/queryMhaRelatedByNames', { params: reqParam })
        .then(response => {
          const replications = response.data.data
          const emptyResult = replications == null || !Array.isArray(replications) || replications.length === 0
          if (emptyResult) {
            this.$Message.info('结果为空')
            return
          }
          replications.forEach((replication) => {
            this.graphData.appendNode(replication.srcMha)
            this.graphData.appendNode(replication.dstMha)
            this.graphData.appendLine(replication)
          })
          if (this.graphData.isEmpty()) {
            return
          }
          if (queryAll) {
            this.$Message.success('查询成功')
          }
          // refresh graph
          this.graphData.appendDataToGraph(queryAll)
          // query delay
          this.getDelay(replications)
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询异常: ' + message)
        })
        .finally(() => {
          this.dataLoading = false
        })
    },
    getDelay (replications) {
      const param = {
        replicationIds: replications.map(item =>
          item.replicationId
        ).join(',')
      }
      console.log(param)
      this.axios.get('/api/drc/v2/replication/delay', { params: param })
        .then(response => {
          const delays = response.data.data
          const emptyResult = delays == null || !Array.isArray(delays) || delays.length === 0
          if (emptyResult) {
            return
          }
          // append graph data
          const dataMap = new Map(delays.map(e => [e.srcMha + '-' + e.dstMha, e.delay]))
          const allLinks = this.graphData.graph.getLinks()
          console.log(allLinks)
          allLinks.forEach(link => {
            link.relations.forEach(line => {
              const repli = line.data.replication
              const delay = dataMap.get(repli.srcMha.name + '-' + repli.dstMha.name)
              let lineColor = 'rgb(3,119,63)'
              let text = null
              if (delay != null) {
                console.log(delay)
                text = prettyMilliseconds(delay, {
                  compact: true
                })
                if (delay > 10000) {
                  lineColor = 'rgb(238,0,0)'
                }
                line.text = text
                line.color = lineColor
              }
            })
          })
          this.graphData.graph.getInstance().dataUpdated()
        })
        .catch(message => {
          console.log(message)
          this.$Message.error('查询延迟异常: ' + message)
        })
    },
    onNodeClick (nodeObject, $event) {
      console.log('onNodeClick:', nodeObject, $event)
      this.showMore([nodeObject.data.mha.name], false)
      this.graphData.deepenClickedNode(nodeObject)
      const mhaName = nodeObject.data.mha.name
      this.$copyText(mhaName).then(e => {
        this.$Message.success('已复制 mha 名称: ' + e.text)
        console.log(e)
      })
      this.showNodeTips(nodeObject, $event)
      this.graphData.nodeMenuPanelPosition.fixed = true
    },
    onLineClick (lineObject, linkObject, $event) {
      console.log(lineObject)
      const replication = lineObject.data.replication
      this.current.link = {
        srcMha: replication.srcMha,
        dstMha: replication.dstMha,
        replicationId: replication.replicationId
      }
      this.showLineMenus(lineObject, $event)
    },
    showLineMenus (nodeObject, $event) {
      if (this.operations.length == null || this.operations.length === 0) {
        return
      }
      const basePosition = this.$refs.myPage.getBoundingClientRect()
      this.graphData.linkMenuPanelPosition.x = $event.clientX - basePosition.x
      this.graphData.linkMenuPanelPosition.y = $event.clientY - basePosition.y + 30
      console.log('showLineMenus:', $event, basePosition)
      setTimeout(() => {
        this.graphData.isShowNodeMenuPanel = true
      }, 10)
    },
    showNodeTips (nodeObject, $event) {
      console.log('showNodeMenus:', nodeObject, $event)
      this.current.node.mha = nodeObject.data.mha
      const basePosition = this.$refs.myPage.getBoundingClientRect()
      this.graphData.isShowNodeTipsPanel = true
      this.graphData.nodeMenuPanelPosition.x = $event.clientX - basePosition.x
      this.graphData.nodeMenuPanelPosition.y = $event.clientY - basePosition.y + 30
    },
    hideNodeTips (nodeObject, $event) {
      console.log('hideNodeTips:', nodeObject, $event)
      if (this.graphData.nodeMenuPanelPosition.fixed) {
        return
      }
      this.graphData.isShowNodeTipsPanel = false
    }
  }
}
</script>
<style lang="scss">
// line
.node-line{
  cursor: pointer;
}

// node
.c-node-menu-item {
  line-height: 30px;
  padding-left: 10px;
  cursor: pointer;
  color: #444444;
  font-size: 14px;
  border-top: #efefef solid 1px;
}

.c-node-menu-item:hover {
  background-color: rgba(66, 187, 66, 0.2);
}

.c-g-center {
  text-align: center;
  cursor: pointer;
}
</style>

<style lang="scss" scoped>

</style>
