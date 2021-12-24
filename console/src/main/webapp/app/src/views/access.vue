<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/access">搭建DRC</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :current="current" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="新建集群" content="新建复制集群" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="断开复制" content="查看复制状态/断开复制连接" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="生成分机房域名" content="生成新/源集群两侧域名" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="建立双向复制" content="配置Replicator和Applier建立集群双向复制" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
          <Step title="录入DalCluster" content="新集群db录入dal cluster" @click.native="jumpTo(4)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="已完成DRC接入" @click.native="jumpTo(5)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <build v-if="current === 0" v-bind="clusterPair" v-on:oldClusterChanged="updateOld" v-on:newClusterChanged="updateNew" v-on:newDrcZoneChanged="updateZone"/>
      <copy v-if="current === 1" v-bind="clusterPair" v-on:oldClusterChanged="updateOld"/>
      <dns v-if="current === 2" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOld"/>
      <drc v-if="current === 3" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOld"/>
      <dal v-if="current === 4" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOld"/>
      <complete v-if="current === 5"/>
      <Divider/>
      <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
        <div>
          <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="current > 0">
            上一步
          </Button>
          <Button type="primary" @click="next" style="position: absolute; left: 790px" v-if="current < 5">
            下一步
          </Button>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>
import build from '../components/access/build.vue'
import copy from '../components/access/copy.vue'
import dns from '../components/access/dns.vue'
import drc from '../components/access/drc.vue'
import dal from '../components/access/dal.vue'
import complete from '../components/access/complete.vue'
export default {
  components: {
    build,
    copy,
    drc,
    dns,
    dal,
    complete
  },
  data () {
    return {
      current: 0,
      clusterPair: {
        oldClusterName: this.$route.query.clustername,
        newClusterName: this.$route.query.newclustername,
        env: '',
        oldZoneId: this.$route.query.zoneId,
        newDrcZone: ''
      }
    }
  },
  methods: {
    jumpTo (n) {
      this.current = n
      this.hasResp = false
    },
    next () {
      this.current = this.current + 1
      this.hasResp = false
    },
    prev () {
      this.current = this.current - 1
      this.hasResp = false
    },
    updateOld (e) {
      this.clusterPair.oldClusterName = e
    },
    updateNew (e) {
      this.clusterPair.newClusterName = e
    },
    updateZone (e) {
      this.clusterPair.newDrcZone = e
    },
    updateEnv (e) {
      this.clusterPair.env = e
    }
  },
  created () {
    const curStep = this.$route.query.step
    if (typeof (curStep) === 'undefined') {
      console.log('curStep undefined, do nothing')
    } else {
      this.jumpTo(Number(curStep))
    }
  }
}
</script>

<style scoped>

</style>
