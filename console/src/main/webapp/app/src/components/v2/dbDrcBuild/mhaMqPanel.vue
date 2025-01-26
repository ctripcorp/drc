<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Collapse v-model="value">
      <Panel v-for="(mhaMqDto, index) in mhaMqDtos"
             :key="mhaMqDto.srcMha.name +'-> mq'"
             :name="String(index)">
        {{ mhaMqDto.srcMha.name + ' ------ ' + ([mhaMqDto.mhaDbReplications.length] + 'DB') }}
        <template #content>
          <Button icon="ios-open-outline" @click="openModal(index)" style="margin-bottom: 10px"> 打开详情</Button>
          <mha-db-mq-panel :mha-mq-dtos="mhaMqDto"/>
              <Modal v-model="openDetailModal[index]" width="1500px" :footer-hide="true">
                <drc-mq-config ref="dbReplicationConfigComponent" v-if="openDetailModal[index]"
                              :mq-type="mhaMqDto.mhaMessengerDto.mqType"
                              :mha-name="mhaMqDto.srcMha.name"
                              :dc="mhaMqDto.srcMha.dcName"
                />
              </Modal>
        </template>
      </Panel>
    </Collapse>
    <br/>
    <Button type="primary" @click="goToSwitchMessengers" icon="md-sync">
      切换Messenger生效配置
    </Button>
    <br/>
  </div>
</template>

<script>

import MhaDbMqPanel from '@/components/v2/dbDrcBuild/mhaDbMqPanel.vue'
import DrcMqConfig from '@/components/v2/mhaMessengers/drcMqConfig.vue'

export default {
  name: 'mhaMqPanel',
  components: { DrcMqConfig, MhaDbMqPanel },
  props: {
    mhaMqDtos: Array
  },
  emits: ['updated'],
  data () {
    return {
      value: [],
      openDetailModal: [],
      dataLoading: false
    }
  },
  methods: {
    openModal (index) {
      console.log(this.mhaMqDtos)
      this.$set(this.openDetailModal, index, true)
    },
    goToSwitchMessengers () {
      this.$Modal.confirm({
        title: '切换Messenger',
        content: '<p>请确认</p>',
        loading: true,
        onOk: () => {
          this.switchMessengers()
        }
      })
    },
    switchMessengers () {
      const params = this.getSwitchParams()
      console.log(params)
      this.dataLoading = true
      this.axios.post('/api/drc/v2/autoconfig/switchMessengers', params)
        .then(response => {
          const data = response.data
          const success = data.status === 0
          if (success) {
            this.$Message.success('提交成功')
          } else {
            this.$Message.warning('提交失败: ' + data.message)
          }
        })
        .catch(message => {
          this.$Message.error('提交异常: ' + message)
        })
        .finally(() => {
          this.$Modal.remove()
          this.dataLoading = false
          this.$emit('updated')
        })
    },
    getSwitchParams () {
      return this.mhaMqDtos.map((item) => {
        return {
          srcMhaName: item.srcMha.name,
          mqType: item.mhaMessengerDto.mqType,
          dbNames: item.mhaDbReplications.map((mhaDbReplication) => {
            return mhaDbReplication.src.dbName
          })
        }
      })
    }
  },
  created () {
    console.log('mhaMqDtos', this.mhaMqDtos)
    this.openDetailModal = Array(this.mhaMqDtos.length).fill(false)
    if (this.mhaMqDtos.length === 1) {
      this.value = ['0']
    }
  }
}
</script>

<style scoped>

</style>
