<template>
      <div>
        <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
            {{title}}
            <span slot="desc" v-html="message"></span>
        </Alert>
        <Form ref="unit" :model="unit" :rules="rulesUnit" :label-width="250" style="margin-top: 50px">
          <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
              <Input v-model="unit.oldClusterName" style="width: 400px" @input="changeOld" placeholder="集群名"/>
          </FormItem>
          <FormItem label="机房区域" prop="drcZone">
            <Select v-model="unit.drcZone" style="width: 200px"  placeholder="选择机房区域" @input="getAllDbs()">
              <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
            </Select>
          </FormItem>
          <FormItem label="选择环境" prop="env">
            <Select v-model="unit.env" style="width: 200px"  placeholder="选择环境" @input="changeEnv">
              <Option v-for="item in envList" :value="item.value" :key="item.value">{{ item.label }}</Option>
            </Select>
          </FormItem>
          <FormItem style="width: 93%" :label-width="75">
            <div>
              <Table size="small" highlight-row border ref="selection" :columns="unit.columns" :data="unit.dbNames" v-model="unit.selectedDbs" @on-selection-change="changeSelection">
                <template slot-scope="{index}" slot="is">
                  <Tag size="large" :color="unit.dbNames[index].isShard ? 'warning':'primary'">{{unit.dbNames[index].isShard ? '是':'否'}}</Tag>
                </template>
                <template slot-scope="{index}" slot="no">
                  <Tag size="large" :color="'default'">{{unit.dbNames[index].shardStartNo}}</Tag>
                </template>
              </Table>
            </div>
          </FormItem>
          <FormItem>
            <Button @click="handleReset('unit')">重置</Button>
            <Button type="primary" @click="changeModal('unit')" style="margin-left: 150px">确定提交</Button>
            <Modal
              v-model="unit.modal"
              title="修改源机房域名"
              @on-ok="postUnit('unit')">
              <p>确定在dal cluster中修改域名为带区域信息的域名吗？</p>
            </Modal>
          </FormItem>
        </Form>
      </div>
</template>
<script>
export default {
  name: 'unit',
  props: {
    oldClusterName: String,
    newClusterName: String,
    env: String,
    oldZoneId: String
  },
  data () {
    return {
      hasResp: false,
      status: '',
      title: '',
      message: '',
      unit: {
        modal: false,
        oldClusterName: this.oldClusterName,
        drcZone: this.oldZoneId,
        env: this.env,
        columns: [
          {
            type: 'selection',
            width: 60,
            align: 'center',
            fixed: 'left'
          },
          {
            title: 'DB',
            key: 'name',
            minWidth: 100,
            fixed: 'left'
          },
          {
            title: 'Dal Cluster',
            key: 'dalName',
            minWidth: 120,
            fixed: 'left'
          },
          {
            title: 'master前域名',
            minWidth: 100,
            key: 'masterBefore'
          },
          {
            title: 'master后域名',
            minWidth: 100,
            key: 'masterAfter'
          },
          {
            title: 'slave前域名',
            minWidth: 100,
            key: 'slaveBefore'
          },
          {
            title: 'slave后域名',
            minWidth: 100,
            key: 'slaveAfter'
          },
          {
            title: 'read前域名',
            minWidth: 100,
            key: 'readBefore'
          },
          {
            title: 'read后域名',
            minWidth: 100,
            key: 'readAfter'
          },
          {
            title: '是否分片',
            minWidth: 70,
            maxWidth: 90,
            slot: 'is'
          },
          {
            title: '分片起始',
            minWidth: 70,
            maxWidth: 90,
            slot: 'no'
          }
        ],
        dbNames: [],
        selectedDbs: []
      },
      rulesUnit: {
        oldClusterName: [
          { required: true, message: '集群名不能为空', trigger: 'blur' }
        ],
        env: [
          { required: true, message: '环境不能为空', trigger: 'blur' }
        ],
        drcZone: [
          { required: true, message: '机房区域不能为空', trigger: 'blur' }
        ]
      },
      drcZoneList: this.constant.dcList,
      envList: [
        {
          value: 'product',
          label: 'PRODUCT'
        },
        {
          value: 'fat',
          label: 'FAT'
        },
        {
          value: 'lpt',
          label: 'LPT'
        },
        {
          value: 'uat',
          label: 'UAT'
        }
      ]
    }
  },
  computed: {
  },
  methods: {
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    postUnit (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          that.hasResp = false
          that.status = 'success'
          that.title = '域名单元化成功!'
          that.message = ''
          for (let i = 0; i < that.unit.selectedDbs.length; i++) {
            that.axios.put('/api/drc/v1/access/domains/unit/clusters/' + that.unit.selectedDbs[i].dalName + '/zones/' + this.unit.drcZone + '/env/' + this.unit.env + '/shards/' + parseInt(this.unit.selectedDbs[i].isShard ? this.unit.selectedDbs[i].shardStartNo : 0), {
              master: {
                beforeDomain: that.unit.selectedDbs[i].masterBefore,
                afterDomain: that.unit.selectedDbs[i].masterAfter
              },
              slave: {
                beforeDomain: that.unit.selectedDbs[i].slaveBefore,
                afterDomain: that.unit.selectedDbs[i].slaveAfter
              },
              read: {
                beforeDomain: that.unit.selectedDbs[i].readBefore,
                afterDomain: that.unit.selectedDbs[i].readAfter
              }
            }).then(response => {
              that.hasResp = true
              if (response.data.data.status === 200) {
                that.message += '域名单元化成功！<br/>DAL集群名：' + that.unit.selectedDbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
              } else {
                that.status = 'error'
                that.title = '域名单元化失败!'
                that.message += '域名单元化失败！<br/>DAL集群名：' + that.unit.selectedDbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
              }
              // loading finish
              this.finish()
            })
          }
        }
      })
    },
    changeEnv () {
      this.$emit('envChanged', this.unit.env)
      this.getAllDbs()
    },
    getAllDbs () {
      this.axios.get('/api/drc/v1/mha/dbnames/dalnames/cluster/' + this.unit.oldClusterName + '/env/' + this.unit.env)
        .then(response => {
          console.log(response.data)
          this.unit.dbNames = []
          response.data.data.forEach((db, index) => {
            this.axios.post('/api/drc/v1/access/db/dns/zoneid/' + this.unit.drcZone, {
              cluster: this.unit.oldClusterName,
              dbname: db.dbname
            }).then(reps => {
              console.log(db)
              const temp = {
                name: db.dbname,
                dalName: db.dalname,
                isShard: db.isshard,
                shardStartNo: db.shardstartno,
                masterBefore: reps.data.data.masterBefore,
                masterAfter: reps.data.data.masterAfter,
                slaveBefore: reps.data.data.slaveBefore,
                slaveAfter: reps.data.data.slaveAfter,
                readBefore: reps.data.data.readBefore,
                readAfter: reps.data.data.readAfter
              }
              this.unit.dbNames.splice(index, 0, temp)
            })
          })
        })
    },
    changeSelection () {
      this.unit.selectedDbs = this.$refs.selection.getSelection()
    },
    changeIsShard (index) {
      if (!this.unit.dbNames[index].isshard) {
        this.unit.dbNames[index].shardStartNo = 0
      }
    },
    changeOld () {
      this.getAllDbs()
      this.$emit('oldClusterChanged', this.unit.oldClusterName)
    },
    start () {
      this.$Loading.start()
    },
    finish () {
      this.$Loading.finish()
    },
    error () {
      this.$Loading.error()
    },
    changeModal (name) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          this.unit.modal = true
        }
      })
    }
  },
  created () {
    this.getAllDbs()
  }
}
</script>
<style scoped>
</style>
