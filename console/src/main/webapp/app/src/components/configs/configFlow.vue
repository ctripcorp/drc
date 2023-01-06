<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/accessV2">DRC配置</BreadcrumbItem>
      <BreadcrumbItem to="/configFlow">高级配置</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#ffffff', margin: '50px 0 1px 185px', zIndex: '1', top: '500px'}">
      <template>
        <Steps :currentStep="currentStep" style="width: 90%; margin-left: 50px; margin-bottom: 15px; margin-top: 50px">
          <Step title="配置表" content="添加同步表" @click.native="jumpTo(0)" :style="{cursor: 'pointer'}"></Step>
          <Step title="表映射" content="配置表映射(非必要）" @click.native="jumpTo(1)" :style="{cursor: 'pointer'}"></Step>
          <Step title="字段映射" content="配置字段映射(非必要）" @click.native="jumpTo(2)" :style="{cursor: 'pointer'}"></Step>
          <Step title="行过滤" content="配置行过滤（非必要）" @click.native="jumpTo(3)" :style="{cursor: 'pointer'}"></Step>
          <Step title="字段过滤" content="配置字段过滤(非必要）" @click.native="jumpTo(4)" :style="{cursor: 'pointer'}"></Step>
          <Step title="完成" content="已完成DRC接入" @click.native="jumpTo(4)" :style="{cursor: 'pointer'}"></Step>
        </Steps>
      </template>
      <table-config v-bind="commonInfo" v-if="currentStep === 0" v-on:namespaceChange="namespaceChange" v-on:nameChange="nameChange"></table-config>
      <table-mapping-config v-bind="commonInfo" v-if="currentStep === 1"></table-mapping-config>
      <columns-mapping-config v-bind="commonInfo" v-if="currentStep === 2"></columns-mapping-config>
      <rows-filter-config v-bind="commonInfo" v-if="currentStep === 3"></rows-filter-config>
      <columns-filter-config v-bind="commonInfo" v-if="currentStep === 4"></columns-filter-config>
      <complete v-bind="commonInfo" v-if="currentStep === 5"></complete>
      <Divider/>
      <div style="padding: 1px 1px; height: 100px; margin-top: 75px">
        <div>
          <Button type="primary" @click="prev" style="position: absolute; left: 465px" v-if="currentStep > 0 ">
            上一步
          </Button>
          <Button type="primary" @click="next" style="position: absolute; left: 790px" v-if="currentStep < 5">
            下一步
          </Button>
        </div>
      </div>
    </Content>
  </base-component>
</template>

<script>
import tableConfig from '@/components/configs/flow/tableConfig'
import tableMappingConfig from '@/components/configs/flow/tableMappingConfig'
import columnsMappingConfig from '@/components/configs/flow/columnsMappingConfig'
import rowsFilterConfig from '@/components/configs/flow/rowsFilterConfig'
import columnsFilterConfig from '@/components/configs/flow/columnsFilterConfig'
import complete from '@/components/configs/flow/complete'

export default {
  name: 'configFlow',
  components: {
    tableConfig,
    tableMappingConfig,
    columnsMappingConfig,
    rowsFilterConfig,
    columnsFilterConfig,
    complete
  },
  data () {
    return {
      currentStep: 0,
      commonInfo: {
        srcMha: '',
        destMha: '',
        applierGroupId: 0,
        srcDc: '',
        destDc: '',
        dataMediaId: 0,
        namespace: '',
        name: ''
      }
    }
  },
  methods: {
    jumpTo (n) {
      this.currentStep = n
    },
    next () {
      this.currentStep = this.currentStep + 1
    },
    prev () {
      this.currentStep = this.currentStep - 1
    },
    namespaceChange (s) {
      this.commonInfo.namespace = s
    },
    nameChange (t) {
      this.commonInfo.name = t
    }
  },
  created () {
    const curStep = this.$route.query.step
    if (curStep == null) {
      console.log('curStep is null, do nothing')
    } else {
      this.jumpTo(Number(curStep))
    }
    this.commonInfo = this.$route.query.commonInfo
  }
}
</script>

<style scoped>

</style>
