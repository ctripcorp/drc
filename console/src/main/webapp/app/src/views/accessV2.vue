<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/accessV2">DRC配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :current="current" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="新建DRC集群" content="新建同步DRC" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="mha配置" content="mha录入db信息" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="预检测" content="检测mha配置" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="建立同步" content="配置Replicator和Applier实例" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="已完成DRC接入" @click.native="jumpTo(4)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <buildV2 v-if="current === 0" v-bind="clusterPair" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster" v-on:newDrcZoneChanged="updateNewZone" v-on:oldDrcZoneChanged="updateOldZone"/>
      <mhaConfig v-if="current === 1" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster"/>
      <preCheck v-if="current === 2" v-bind="clusterPair" />
      <drc v-if="current === 3" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:oldClusterChanged="updateOldCluster" v-on:newClusterChanged="updateNewCluster" />
      <complete v-if="current === 4"/>
      <Divider/>
      <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
        <div>
          <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="current > 0 ">
            上一步
          </Button>
          <Button type="primary" @click="next" style="position: absolute; left: 790px" v-if="current < 4">
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
import preCheck from '../components/accessV2/preCheck.vue'
import drc from '../components/access/drc.vue'
import complete from '../components/access/complete.vue'
export default {
  components: {
    buildV2,
    mhaConfig,
    preCheck,
    drc,
    complete
  },
  data () {
    return {
      current: 0,
      clusterPair: {
        // due to leagcy, oldClusterName means first mhaName in mhaGroup
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
      // this.getDcName()
      this.jumpTo(Number(curStep))
    }
    // 标识 mha name 的顺序，用于返回
    const order = this.$route.query.order
    if (order == null) {
      console.log('order is null, do nothing')
      this.clusterPair.oldClusterName = this.$route.query.clustername
      this.clusterPair.newClusterName = this.$route.query.newclustername
    } else {
      if (order) {
        this.clusterPair.oldClusterName = this.$route.query.clustername
        this.clusterPair.newClusterName = this.$route.query.newclustername
      } else {
        this.clusterPair.oldClusterName = this.$route.query.newclustername
        this.clusterPair.newClusterName = this.$route.query.clustername
      }
    }
  }
}
</script>

<style scoped>

</style>
