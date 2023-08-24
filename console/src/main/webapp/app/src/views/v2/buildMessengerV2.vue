<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/v2/buildMessengerV2">Messenger配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :current="current" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="录入Mha" content="" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="mha配置" content="mha录入db信息" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="预检测" content="检测mha配置" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="建立同步" content="配置Replicator和Messenger实例" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <mhaInit v-if="current === 0" v-bind="sharedInfo" v-on:mhaNameChanged="updateMhaName" v-on:dcChanged="updateDc"/>
      <mhaConfig v-if="current === 1" v-bind="sharedInfo" v-on:mhaNameChanged="updateMhaName" v-on:dcChanged="updateDc"/>
      <preCheck v-if="current === 2" v-bind="sharedInfo"/>
      <drcConfig v-if="current === 3" v-bind="sharedInfo"/>
      <Divider/>
      <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
        <div>
          <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="current > 0 ">
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
import mhaInit from '../../components/v2/mhaMessengers/mhaInit.vue'
import mhaConfig from '../../components/v2/mhaMessengers/mhaConfig.vue'
import preCheck from '../../components/v2/mhaMessengers/preCheck.vue'
import drcConfig from '../../components/v2/mhaMessengers/drcConfig.vue'
export default {
  name: 'buildMhaMessenger.vue',
  components: {
    mhaInit,
    mhaConfig,
    preCheck,
    drcConfig
  },
  data () {
    return {
      current: 0,
      sharedInfo: {
        mhaName: this.$route.query.mhaName,
        dc: ''
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
    updateMhaName (e) {
      this.sharedInfo.mhaName = e.trim()
    },
    updateDc (e) {
      this.sharedInfo.dc = e
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
