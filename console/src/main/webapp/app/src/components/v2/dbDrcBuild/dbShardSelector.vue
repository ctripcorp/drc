<template>
  <div class="db-shard-selector">
    <RadioGroup v-model="internalValue" button-style="solid" @on-change="handleChange">
      <Radio v-for="items in dbNamesGroup" :key="joinArray(items)" :label="joinArray(items)" border>
        {{ `[${items.length}]: ${formatShardRanges(joinArray(items))}` }}
      </Radio>
    </RadioGroup>
    <Button type="primary" icon="md-add" ghost @click="handleCustomDb">
      拆分
    </Button>
    <Modal v-model="showCreateModal" width="1200px" :footer-hide="true" title="选择部分DB">
      <Collapse v-model="openedDbGroupIndex" accordion>
        <Panel v-for="(items, index) in dbNamesGroup"
               :key="joinArray(items)"
               :name="String(index)">
          {{ `[${items.length}]: ${formatShardRanges(joinArray(items))}` }}
          <template #content>
            <!--            <mha-db-replication-panel :mha-replication="replication"/>-->
            <CheckboxGroup v-model="checkedDbNamesByGroup[index]">
              <Checkbox v-for="dbName in items" :key="dbName" :label="dbName">
                {{ dbName }}
              </Checkbox>
            </CheckboxGroup>
          </template>
        </Panel>
      </Collapse>
      <Divider></Divider>
      <Button type="primary" icon="md-add" @click="handleDbUpdated" style="margin-left: 50px">
        确定
      </Button>
    </Modal>
  </div>
</template>

<script>

export default {
  name: 'DbShardSelector',
  components: {},
  props: {
    allDrcConfigs: {
      type: Array,
      required: true
    }
  },
  data () {
    return {
      internalValue: '',
      initialized: false,
      showCreateModal: false,
      openedDbGroupIndex: '0',
      dbNamesGroup: [],
      checkedDbNamesByGroup: []
    }
  },
  methods: {
    formatShardRanges (input) {
      const shards = input.split(',')

      // 分离带数字和不带数字的分片
      const numberedShards = []
      const specialShards = []

      shards.forEach(shard => {
        const match = shard.match(/(\d+)/)
        if (match) {
          numberedShards.push({
            fullName: shard,
            num: parseInt(match[1], 10)
          })
        } else {
          specialShards.push(shard)
        }
      })

      // 处理带数字的分片
      numberedShards.sort((a, b) => a.num - b.num)

      const ranges = []
      if (numberedShards.length > 0) {
        const prefix = numberedShards[0].fullName.split(/\d+/)[0]
        const suffix = numberedShards[0].fullName.split(/\d+/)[1]

        let currentStart = numberedShards[0].num
        let currentEnd = numberedShards[0].num

        for (let i = 1; i < numberedShards.length; i++) {
          if (numberedShards[i].num === currentEnd + 1) {
            currentEnd = numberedShards[i].num
          } else {
            if (currentStart === currentEnd) {
              ranges.push(`${prefix}${currentStart.toString().padStart(2, '0')}${suffix}`)
            } else {
              ranges.push(`${prefix}${currentStart.toString().padStart(2, '0')}${suffix}~${prefix}${currentEnd}${suffix}`)
            }
            currentStart = numberedShards[i].num
            currentEnd = numberedShards[i].num
          }
        }

        // 处理最后一个范围
        if (currentStart === currentEnd) {
          ranges.push(`${prefix}${currentStart.toString().padStart(2, '0')}${suffix}`)
        } else {
          ranges.push(`${prefix}${currentStart.toString().padStart(2, '0')}${suffix}~${prefix}${currentEnd}${suffix}`)
        }
      }

      // 合并结果
      return [...ranges, ...specialShards].join(', ')
    },
    handleChange (value) {
      this.$emit('change', value)
    },
    joinArray (arr) {
      if (!arr || arr.length === 0) {
        return ''
      }
      return arr.join(',')
    },

    handleCustomDb () {
      this.showCreateModal = true
    },
    handleDbUpdated () {
      console.log('this.openedDbGroupIndex', this.openedDbGroupIndex)
      const checkedDbNames = this.checkedDbNamesByGroup[this.openedDbGroupIndex]
      if (!checkedDbNames || checkedDbNames.length === 0) {
        this.$Message.warning('请至少选择一个DB')
        return
      }
      if (checkedDbNames.length === this.dbNamesGroup[this.openedDbGroupIndex].length) {
        this.$Message.warning('请勿全选')
        return
      }
      this.dbNamesGroup[this.openedDbGroupIndex] = this.dbNamesGroup[this.openedDbGroupIndex].filter(item => !checkedDbNames.includes(item))
      this.dbNamesGroup.push(checkedDbNames)
      this.internalValue = this.joinArray(checkedDbNames)
      this.handleChange(this.internalValue)
      // clean up
      this.showCreateModal = false
      this.checkedDbNamesByGroup = []
      this.openedDbGroupIndex = '0'
    },
    refreshDbGroup () {
      if (!this.allDrcConfigs) {
        return []
      }
      this.dbNamesGroup = this.allDrcConfigs.map((dbDrcConfig) => {
        return dbDrcConfig.dbNames
      })
    }
  },
  watch: {
    allDrcConfigs: {
      immediate: true, // 立即执行一次
      handler (newVal) {
        this.refreshDbGroup()
        if (!this.isInitialized && newVal.length >= 1) {
          this.internalValue = this.joinArray(newVal[0].dbNames)
          this.isInitialized = true
          console.log('aa Initialized with:', this.internalValue)
        }
      }
    }
  },
  computed: {}
}
</script>

<style scoped>
.db-shard-selector {
  display: flex;
  align-items: center;
  gap: 10px;
}
</style>
