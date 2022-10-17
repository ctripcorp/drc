<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/buildMessenger">Messenger配置</BreadcrumbItem>
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
export default {
  name: 'buildMhaMessenger.vue'
}
</script>

<style scoped>

</style>
