<template>
  <div :style="{padding: '1px 1px',height: '100%'}">
    <Collapse v-model="value">
      <Panel v-for="(replication, index) in mhaReplications"
             :key="replication.srcMha.name +'->'+replication.dstMha.name"
             :name="String(index)">
        {{ replication.srcMha.name + ' -> ' + replication.dstMha.name + '     ------   ['+ ([replication.mhaDbReplications.length] +'DB]')}}
        <template #content>
          <Button icon="ios-open-outline" @click="openModal(index)" style="margin-bottom: 10px"> 打开详情</Button>
          <mha-db-replication-panel :mha-replication="replication"/>
          <Modal v-model="openDetailModal[index]" width="1500px" :footer-hide="true">
            <drc-build-v2 ref="dbReplicationConfigComponent" v-if="openDetailModal[index]"
                          :src-mha-name="replication.srcMha.name"
                          :src-dc="replication.srcMha.dcName"
                          :dst-mha-name="replication.dstMha.name"
                          :dst-dc="replication.dstMha.dcName"
            />
          </Modal>
        </template>
      </Panel>
    </Collapse>
    <br/>
    <Button type="primary" @click="goToSwitchAppliers" icon="md-sync">
      切换Applier生效配置
    </Button>
  </div>
</template>

<script>
import MhaDbReplicationPanel from '@/components/v2/dbDrcBuild/mhaDbReplicationPanel.vue'
import DrcBuildV2 from '@/views/v2/meta/buildStep/drcBuildV2.vue'
import mhaDbReplications from '@/views/v2/meta/mhaDbReplications.vue'

export default {
  name: 'mhaReplicationPanel',
  components: { DrcBuildV2, MhaDbReplicationPanel },
  props: {
    mhaReplications: Array
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
      this.$set(this.openDetailModal, index, true)
    },
    goToSwitchAppliers () {
      this.$Modal.confirm({
        title: '切换Applier',
        content: '<p>请确认</p>',
        loading: true,
        onOk: () => {
          this.switchAppliers()
        }
      })
    },
    switchAppliers () {
      const params = this.getSwitchParams()
      console.log(params)
      this.dataLoading = true
      this.axios.post('/api/drc/v2/autoconfig/switchAppliers', params)
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
      return this.mhaReplications.map((item) => {
        return {
          srcMhaName: item.srcMha.name,
          dstMhaName: item.dstMha.name,
          dbNames: item.mhaDbReplications.map((mhaDbReplication) => {
            return mhaDbReplication.src.dbName
          })
        }
      })
    }
  },
  created () {
    this.openDetailModal = Array(mhaDbReplications.length).fill(false)
  }
}
</script>

<style scoped>

</style>
