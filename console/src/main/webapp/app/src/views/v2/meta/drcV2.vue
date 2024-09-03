<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem :to="{
          path: '/v2/mhaReplications',query :{
          srcMhaName: this.clusterPair.srcMhaName,
          dstMhaName: this.clusterPair.dstMhaName,
          preciseSearchMode: true
        }
      }">首页
      </BreadcrumbItem>
      <BreadcrumbItem to="/drcV2">DRC配置V2</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :current="current" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="新建DRC集群" content="新建同步DRC" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="mha配置" content="mha录入db信息" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="预检测" content="检测mha配置" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="建立同步" content="配置Replicator和Applier实例" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <mhaBuild v-if="current === 0" v-bind="clusterPair" v-on:srcMhaNameChanged="updateSrcMha" v-on:dstMhaNameChanged="updateDstMha" v-on:dstDcChanged="updateDstDc" v-on:srcDcChanged="updateSrcDc"/>
      <mha-config-v2 v-if="current === 1" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:srcMhaNameChanged="updateSrcMha" v-on:dstMhaNameChanged="updateDstMha"/>
      <pre-check-v2 v-if="current === 2" v-bind="clusterPair" />
      <drc-build-v2 v-if="current === 3" v-bind="clusterPair" v-on:envChanged="updateEnv" v-on:srcMhaNameChanged="updateSrcMha" v-on:dstMhaNameChanged="updateDstMha" />
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
import mhaBuild from './buildStep/mhaBuild.vue'
import mhaConfigV2 from './buildStep/mhaConfigV2.vue'
import preCheckV2 from './buildStep/preCheckV2.vue'
import drcBuildV2 from './buildStep/drcBuildV2.vue'
export default {
  components: {
    mhaBuild,
    mhaConfigV2,
    preCheckV2,
    drcBuildV2
  },
  props: {
    srcMhaName: String,
    dstMhaName: String,
    srcDc: String,
    dstDc: String,
    env: String
  },
  data () {
    return {
      current: 0,
      clusterPair: {
        srcMhaName: '',
        dstMhaName: '',
        env: '',
        srcDc: '',
        dstDc: ''
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
    updateSrcMha (e) {
      this.clusterPair.srcMhaName = e
    },
    updateDstMha (e) {
      this.clusterPair.dstMhaName = e
    },
    updateSrcDc (e) {
      this.clusterPair.srcDc = e
    },
    updateDstDc (e) {
      this.clusterPair.dstDc = e
    },
    updateEnv (e) {
      this.clusterPair.env = e
    }
  },
  created () {
    this.axios.get('/api/drc/v2/permission/meta/mhaReplication/modify').then((response) => {
      if (response.data.status === 403) {
        this.$router.push('/nopermission')
        return
      }
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
        this.clusterPair.srcMhaName = this.$route.query.srcMhaName
        this.clusterPair.dstMhaName = this.$route.query.dstMhaName
        this.clusterPair.srcDc = this.$route.query.srcDc
        this.clusterPair.dstDc = this.$route.query.dstDc
      } else {
        if (order) {
          this.clusterPair.srcMhaName = this.$route.query.srcMhaName
          this.clusterPair.dstMhaName = this.$route.query.dstMhaName
          this.clusterPair.srcDc = this.$route.query.srcDc
          this.clusterPair.dstDc = this.$route.query.dstDc
        } else {
          this.clusterPair.srcMhaName = this.$route.query.dstMhaName
          this.clusterPair.dstMhaName = this.$route.query.srcMhaName
        }
      }
    })
  }
}
</script>

<style scoped>

</style>
