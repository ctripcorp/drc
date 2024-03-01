<template>
  <Collapse v-model="value">
    <Panel v-for="(replication, index) in mhaReplications"
           :key="replication.srcMha.name +'->'+replication.dstMha.name"
           :name="String(index)">
      {{ replication.srcMha.name + ' -> ' + replication.dstMha.name }}
      <template #content>
        <Button icon="ios-open-outline" @click="openModal(index)" style="margin-bottom: 10px"> 打开详情</Button>
        <mha-db-replication-panel :mha-db-replications="replication.mhaDbReplications"/>
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
  data () {
    return {
      value: ['0'],
      openDetailModal: []
    }
  },
  methods: {
    openModal (index) {
      this.$set(this.openDetailModal, index, true)
    }
  },
  created () {
    this.openDetailModal = Array(mhaDbReplications.length).fill(false)
  }
}
</script>

<style scoped>

</style>
