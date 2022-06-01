<template v-if="current === 0" :key="0">
  <div>
    <Alert :type="status" show-icon v-if="hasResp" style="width: 65%; margin-left: 250px">
      {{title}}
      <span slot="desc" v-html="message"></span>
    </Alert>
    <Form ref="dal" :model="dal" :rules="ruleDal" :label-width="250" style="margin-top: 50px">
      <FormItem label="源集群名" prop="oldClusterName" style="width: 600px">
        <Input v-model="dal.oldClusterName" @input="changeOld" placeholder="请输入源集群名"/>
      </FormItem>
      <FormItem label="新集群名" prop="newClusterName" style="width: 600px">
        <Input v-model="dal.newClusterName" placeholder="请输入新集群名"/>
      </FormItem>
      <FormItem label="新集群机房区域" prop="drcZone">
        <Select v-model="dal.drcZone" style="width: 200px"  placeholder="选择机房区域" @input="getAllDbs">
          <Option v-for="item in dal.drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="选择环境" prop="env">
        <Select v-model="dal.env" style="width: 200px"  placeholder="选择环境" @input="changeEnv">
          <Option v-for="item in dal.envList" :value="item.value" :key="item.value">{{ item.label }}</Option>
        </Select>
      </FormItem>
      <FormItem label="选择DAL集群" style="width:90%">
        <div>
          <Table highlight-row border ref="selection" :columns="dal.columns" :data="dal.dbNames" v-model="dal.selectedDbs" @on-selection-change="changeSelection">
            <template slot-scope="{index}" slot="status">
              <Tag size="large" :color="dal.dbNames[index].status ? 'success':'default'">{{dal.dbNames[index].status ? '已录入':'未录入'}}</Tag>
            </template>
            <template slot-scope="{index}" slot="is">
              <Tag size="large" :color="dal.dbNames[index].isShard ? 'warning':'primary'">{{dal.dbNames[index].isShard ? '是':'否'}}</Tag>
            </template>
            <template slot-scope="{index}" slot="no">
              <Tag size="large" :color="'default'">{{dal.dbNames[index].shardStartNo}}</Tag>
            </template>
          </Table>
        </div>
      </FormItem>
      <FormItem>
        <Button @click="handleReset('dal')">重置</Button>
        <Button type="primary" style="margin-left: 150px" @click="changeModal('dal', 1)">新集群录入DAL Cluster</Button>
        <Modal
          v-model="dal.modal1"
          title="录入DAL集群"
          @on-ok="postDal('dal', 'instance')">
          <p>确定将所有已选的db录入dal cluster吗？</p>
        </Modal>
<!--        <Button type="primary" @click="changeModal('dal', 2)" style="margin-left: 50px">录入用户</Button>-->
<!--        <Modal-->
<!--          v-model="dal.modal2"-->
<!--          title="录入用户"-->
<!--          @on-ok="postDal('dal', 'user')">-->
<!--          <p>确定将所有已选db的用户录入dal cluster吗？</p>-->
<!--        </Modal>-->
<!--        <Button type="success" @click="changeModal('dal', 3)" style="margin-left: 50px">发布</Button>-->
<!--        <Modal-->
<!--          v-model="dal.modal3"-->
<!--          title="发布dal cluster"-->
<!--          @on-ok="release('dal')">-->
<!--          <p>确定发布所有已选的dal cluster吗？</p>-->
<!--        </Modal>-->
      </FormItem>
    </Form>
  </div>
</template>
<script>
export default {
  name: 'dal',
  props: {
    oldClusterName: String,
    newClusterName: String,
    env: String,
    newDrcZone: String
  },
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      dal: {
        modal1: false,
        modal2: false,
        modal3: false,
        oldClusterName: this.oldClusterName,
        newClusterName: this.newClusterName,
        env: this.env,
        drcZone: this.newDrcZone,
        columns: [
          {
            type: 'selection',
            width: 60,
            align: 'center'
          },
          {
            title: 'DB',
            minWidth: 150,
            key: 'name'
          },
          {
            title: 'Dal Cluster',
            minWidth: 150,
            key: 'dalName'
          },
          {
            title: '录入状态',
            width: 110,
            slot: 'status'
          },
          {
            title: '是否分片',
            width: 100,
            slot: 'is'
          },
          {
            title: '分片起始',
            width: 100,
            slot: 'no'
          }
        ],
        dbNames: [],
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
        ],
        drcZoneList: [
          {
            value: 'shaoy',
            label: '上海欧阳'
          },
          {
            value: 'shaxy',
            label: '上海新源'
          },
          {
            value: 'sharb',
            label: '上海日版'
          },
          {
            value: 'shajq',
            label: '上海金桥'
          },
          {
            value: 'shafq',
            label: '上海福泉'
          },
          {
            value: 'shajz',
            label: '上海金钟'
          },
          {
            value: 'ntgxh',
            label: '南通星湖大道'
          },
          {
            value: 'fraaws',
            label: '法兰克福AWS'
          },
          {
            value: 'shali',
            label: '上海阿里'
          },
          {
            value: 'sinibuaws',
            label: 'IBU-VPC'
          },
          {
            value: 'sinibualiyun',
            label: 'IBU-VPC(aliyun)'
          },
          {
            value: 'sinaws',
            label: '新加坡AWS'
          }
        ]
      },
      ruleDal: {
        dalClusterName: [
          { required: true, message: 'DAL集群名不能为空', trigger: 'blur' }
        ],
        oldClusterName: [
          { required: true, message: '源集群名不能为空', trigger: 'blur' }
        ],
        newClusterName: [
          { required: true, message: '新集群名不能为空', trigger: 'blur' }
        ],
        env: [
          { required: true, message: '环境不能为空', trigger: 'blur' }
        ],
        drcZone: [
          { required: true, message: '选择新建集群区域', trigger: 'change' }
        ]
      }
    }
  },
  methods: {
    postDal (name, goal) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          const dbs = this.dal.selectedDbs
          that.hasResp = false
          that.message = ''
          that.title = '录入成功!'
          that.status = 'success'
          for (let i = 0; i < dbs.length; i++) {
            // // old cluster
            // that.axios.post('/api/drc/v1/access/clusters/env/' + that.dal.env + '/goal/' + goal, {
            //   dalclustername: dbs[i].dalName,
            //   dbname: dbs[i].name,
            //   clustername: that.dal.oldClusterName,
            //   isshard: dbs[i].isShard,
            //   shardstartno: dbs[i].isShard ? parseInt(dbs[i].shardStartNo) : 0
            // }).then(response => {
            //   that.hasResp = true
            //   if (response.data.data.success) {
            //     if (goal === 'instance') {
            //       that.message += 'DAL集群录入成功！<br/>集群名：' + that.dal.oldClusterName + '：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
            //     } else if (goal === 'user') {
            //       that.message += '用户录入成功！<br/>集群名：' + that.dal.oldClusterName + '：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
            //     }
            //   } else {
            //     that.title = '失败!'
            //     that.status = 'error'
            //     if (goal === 'instance') {
            //       that.message += 'DAL集群录入失败！<br/>集群名：' + that.dal.oldClusterName + '：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
            //     } else if (goal === 'user') {
            //       that.message += '用户录入失败！<br/>集群名：' + that.dal.oldClusterName + '：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
            //     }
            //   }
            //   // loading finish
            //   this.finish()
            // })
            // new cluster
            that.axios.post('/api/drc/v1/access/clusters/env/' + that.dal.env + '/goal/' + goal, {
              dalclustername: dbs[i].dalName,
              dbname: dbs[i].name,
              basedbname: dbs[i].basedbname,
              clustername: that.dal.newClusterName,
              isshard: dbs[i].isShard,
              shardstartno: dbs[i].isShard ? parseInt(dbs[i].shardStartNo) : 0
            }).then(response => {
              that.hasResp = true
              if (response.data.data.success) {
                if (goal === 'instance') {
                  that.message += 'DAL集群录入成功！<br/>集群名：' + that.dal.newClusterName + ':' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                } else if (goal === 'user') {
                  that.message += '用户录入成功！<br/>集群名：' + that.dal.newClusterName + ':' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                }
              } else {
                that.title = '失败!'
                that.status = 'error'
                if (goal === 'instance') {
                  that.message += 'DAL集群录入失败！<br/>集群名：' + that.dal.newClusterName + ':' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                } else if (goal === 'user') {
                  that.message += '用户录入失败！<br/>集群名：' + that.dal.newClusterName + ':' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                }
              }
              // loading finish
              this.finish()
            })
          }
        }
      })
    },
    release (name) {
      const that = this
      that.$refs[name].validate((valid) => {
        if (!valid) {
          that.$Message.error('仍有必填项未填!')
        } else {
          // loading start
          this.start()
          const dbs = this.dal.selectedDbs
          that.hasResp = false
          that.status = 'success'
          that.title = '发布DAL集群成功!'
          that.message = ''
          for (let i = 0; i < dbs.length; i++) {
            that.axios.post('/api/drc/v1/access/clusters/clustername/' + dbs[i].dalName + '/env/' + that.dal.env + '/releases')
              .then(response => {
                that.hasResp = true
                if (response.data.data.status === 200) {
                  that.message += '发布成功！<br/>集群名：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                } else {
                  that.status = 'error'
                  that.title = '发布DAL集群失败!'
                  that.message += '发布失败！<br/>集群名：' + dbs[i].dalName + '<br/>提示信息：' + response.data.data.message + '<hr/>'
                }
                // loading finish
                this.finish()
              })
          }
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    changeEnv () {
      this.$emit('envChanged', this.dal.env)
      this.getAllDbs()
    },
    getAllDbs () {
      this.axios.get('/api/drc/v1/mha/dbnames/dalnames/cluster/' + this.dal.oldClusterName + '/env/' + this.dal.env + '/zoneId/' + this.dal.drcZone)
        .then(response => {
          console.log(response.data)
          this.dal.dbNames = []
          response.data.data.forEach(db => this.dal.dbNames.push({
            name: db.dbname,
            basedbname: db.basedbname,
            dalName: db.dalname,
            isShard: db.isshard,
            shardStartNo: db.shardstartno,
            status: db.status
          }))
        })
    },
    changeSelection () {
      this.dal.selectedDbs = this.$refs.selection.getSelection()
    },
    changeOld () {
      this.getAllDbs()
      this.$emit('oldClusterChanged', this.dal.oldClusterName)
    },
    changeIsShard (index) {
      if (!this.dal.dbNames[index].isshard) {
        this.dal.dbNames[index].shardStartNo = 0
      }
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
    changeModal (name, i) {
      this.$refs[name].validate((valid) => {
        if (!valid) {
          this.$Message.error('仍有必填项未填!')
        } else {
          switch (i) {
            case (1): this.dal.modal1 = true; break
            case (2): this.dal.modal2 = true; break
            case (3): this.dal.modal3 = true; break
          }
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
