<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/access">搭建DRC</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :current="current" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="新建DRC集群" content="新建复制集群" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="mha配置" content="mha添加db信息" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="建立双向复制" content="配置Replicator和Applier建立集群双向复制" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="已完成DRC接入" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <buildV2 v-if="current === 0" v-bind="clusterPair" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster" v-on:newDrcZoneChanged="updateNewZone" v-on:oldDrcZoneChanged="updateOldZone"/>
      <mhaConfig v-if="current === 1" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster"/>
      <drc v-if="current === 2" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster"/>
      <complete v-if="current === 3"/>
      <Divider/>
      <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
        <div>
          <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="current > 0">
            上一步
          </Button>
          <Button type="primary" @click="next" style="position: absolute; left: 790px" v-if="current < 3">
            下一步
          </Button>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>
import buildV2 from '../components/accessV2/buildV2.vue'
import mhaConfig from '../components/accessV2/mhaConfig.vue'
import drc from '../components/access/drc.vue'
import complete from '../components/access/complete.vue'
export default {
  components: {
    buildV2,
    mhaConfig,
    drc,
    complete
  },
  data () {
    return {
      current: 0,
      clusterPair: {
        oldClusterName: '',
        newClusterName: '',
        env: '',
        oldDrcZone: '',
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
    updateOldCluster (e) {
      this.clusterPair.oldClusterName = e
    },
    updateNewCluster (e) {
      this.clusterPair.newClusterName = e
    },
    updateOldZone (e) {
      this.clusterPair.oldDrcZone = e
    },
    updateNewZone (e) {
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
