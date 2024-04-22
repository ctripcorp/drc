<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/applicationForm">DRC审批</BreadcrumbItem>
      <BreadcrumbItem>DRC申请</BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div style="padding: 1px 1px">
        <Form ref="buildParam" :model="buildParam" :rules="ruleValidate" :label-width="140" :disabled="showDetail">
          <FormItem label="同步DB" prop="dbName">
            <Input v-model="buildParam.dbName" placeholder="输入同步DB"></Input>
          </FormItem>
          <FormItem label="同步表" prop="tableName">
            <Input v-model="buildParam.tableName" type="textarea" :autosize="{minRows: 1,maxRows: 5}" placeholder="输入同步表,多表用,分隔; 不填默认同步所有表"></Input>
          </FormItem>
          <FormItem label="同步方向" :required=true>
            <Row>
              <Col span="11">
                <Card :bordered="true">
                  <template #title>
                    <Icon type="ios-pin"/>
                    源 region
                  </template>
                  <FormItem prop="srcRegion">
                    <Select filterable clearable v-model="buildParam.srcRegion" placeholder="地域">
                      <Option v-for="region in regionOptions" :value="region" :key="region" :label="region">
                        {{ region }}
                      </Option>
                    </Select>
                  </FormItem>
                </Card>
              </Col>
              <Col span="2" style="text-align: center">
                <Button size="middle" shape="circle" type="default" @click="reverseRegion()">
                  ->
                </Button>
              </Col>
              <Col span="11">
                <Card :bordered="true">
                  <template #title>
                    <Icon type="ios-pin"/>
                    目标 region
                  </template>
                  <FormItem prop="dstRegion">
                    <Select filterable clearable v-model="buildParam.dstRegion" placeholder="地域" prop="dstRegion">
                      <Option v-for="region in regionOptions" :value="region" :key="region" :label="region">
                        {{ region }}
                      </Option>
                    </Select>
                  </FormItem>
                </Card>
              </Col>
            </Row>
          </FormItem>
          <FormItem label="过滤方式" prop="filterType">
            <Select v-model="buildParam.filterType">
              <Option v-for="item in filterTypeOpts" :value="item" :key="item">{{item}}</Option>
            </Select>
          </FormItem>
          <FormItem label="BU" prop="buName">
            <Select filterable prefix="ios-home" v-model="buildParam.buName" @on-change="autoSetTag" placeholder="部门">
              <Option v-for="item in bus" :value="item.buName" :key="item.buName">{{item.buName}}</Option>
            </Select>
          </FormItem>
          <FormItem label="Tag" prop="tag">
            <Select v-model="buildParam.tag">
              <Option v-for="item in tags" :value="item" :key="item">{{item}}</Option>
            </Select>
          </FormItem>
          <FormItem label="是否刷存量数据" prop="flushExistingData">
            <RadioGroup v-model="buildParam.flushExistingData">
              <Radio label='0'>否</Radio>
              <Radio label='1'>是</Radio>
            </RadioGroup>
          </FormItem>
          <FormItem label="是否订单相关" prop="orderRelated">
            <RadioGroup v-model="buildParam.orderRelated">
              <Radio label='0'>否</Radio>
              <Radio label='1'>是</Radio>
            </RadioGroup>
          </FormItem>
          <FormItem label="同步开始位点" prop="gtidInit">
            <Input v-model="buildParam.gtidInit" type="textarea" :autosize="{minRows: 2,maxRows: 5}" placeholder="输入同步开始位点"></Input>
          </FormItem>
          <FormItem label="中断影响" prop="disruptionImpact">
            <Input v-model="buildParam.disruptionImpact" type="textarea" :autosize="{minRows: 1,maxRows: 2}" placeholder="中断影响"></Input>
          </FormItem>
          <FormItem label="tps" prop="tps">
            <Input v-model="buildParam.tps" placeholder="tps"></Input>
          </FormItem>
          <FormItem label="申请人" prop="applicant">
            <Input v-model="buildParam.applicant" placeholder="申请人"></Input>
          </FormItem>
          <FormItem label="备注" prop="remark">
            <Input v-model="buildParam.remark" type="textarea" :autosize="{minRows: 2,maxRows: 5}" placeholder="备注"></Input>
          </FormItem>
          <FormItem v-if="!showDetail">
            <Button type="primary" @click="handleSubmit('buildParam')">提交</Button>
            <Button @click="handleReset('buildParam')" style="margin-left: 20px">重置</Button>
          </FormItem>
        </Form>
        <Button v-if="showDetail" style="margin-left: 80px" type="primary" @click="toDrcBuild()">审批</Button>
      </div>
    </Content>
  </base-component>
</template>

<script>

export default {
  name: 'applicationBuild',
  data () {
    return {
      applicationFormId: null,
      buildLoading: false,
      showDetail: false,
      bus: [],
      tags: this.constant.tagList,
      regionOptions: [],
      filterTypeOpts: ['ALL', 'UDL'],
      replicationTypeOpts: [
        {
          name: '单向同步',
          val: 0
        },
        {
          name: '双向同步',
          val: 1
        }
      ],
      buildParam: {
        dbName: '',
        tableName: '',
        srcRegion: '',
        dstRegion: '',
        filterType: 'ALL',
        buName: '',
        tps: '',
        description: '',
        disruptionImpact: '',
        tag: '',
        flushExistingData: '0',
        orderRelated: '0',
        gtidInit: '',
        remark: '',
        applicant: ''
      },
      ruleValidate: {
        dbName: [
          { required: true, message: '请输入同步DB', trigger: 'blur' }
        ],
        // tableName: [
        //   { required: true, message: '请输入同步表', trigger: 'blur' }
        // ],
        applicant: [
          { required: true, message: '请输入申请人', trigger: 'blur' }
        ],
        filterType: [
          { required: true, message: '请选择过滤类型', trigger: 'change' }
        ],
        flushExistingData: [
          { required: true, message: '请选择是否刷存量数据', trigger: 'change' }
        ],
        orderRelated: [
          { required: true, message: '请选择是否订单相关', trigger: 'change' }
        ],
        srcRegion: [
          { required: true, message: '请选择源region', trigger: 'change' }
        ],
        dstRegion: [
          { required: true, message: '请选择目标region', trigger: 'change' }
        ],
        buName: [
          { required: true, message: '请选择BU', trigger: 'change' }
        ],
        tag: [
          { required: true, message: '请选择tag', trigger: 'change' }
        ]
      }
    }
  },
  methods: {
    reverseRegion () {
      const srcRegion = this.buildParam.srcRegion
      this.buildParam.srcRegion = this.buildParam.dstRegion
      this.buildParam.dstRegion = srcRegion
    },
    getRegions () {
      this.axios.get('/api/drc/v2/autoconfig/region/all')
        .then(response => {
          this.regionOptions = response.data.data
        })
    },
    autoSetTag () {
      if (this.buildParam.buName === 'FLT') {
        this.buildParam.tag = 'FLT'
      } else if (this.buildParam.buName === 'HTL') {
        this.buildParam.tag = 'HTL'
      } else {
        this.buildParam.tag = 'COMMON'
      }
    },
    getBus () {
      this.axios.get('/api/drc/v2/meta/bus/all')
        .then(response => {
          this.bus = response.data.data
        })
    },
    toDrcBuild () {
      this.axios.post('/api/drc/v2/application/approve?applicationFormId=' + this.applicationFormId)
        .then(res => {
          if (res.data.status === 1) {
            this.$Message.error(res.data.message)
          } else {
            this.$router.push({
              path: '/drcBuild',
              query: {
                dalClusterName: this.buildParam.dbName + '_dalcluster',
                tableName: this.changeTableName(this.buildParam.tableName),
                srcRegion: this.buildParam.srcRegion,
                dstRegion: this.buildParam.dstRegion,
                filterType: this.buildParam.filterType,
                buName: this.buildParam.buName,
                tag: this.buildParam.tag,
                flushExistingData: this.buildParam.flushExistingData,
                gtidInit: this.buildParam.gtidInit,
                applicationFormId: this.applicationFormId
              }
            })
          }
        })
    },
    changeTableName (name) {
      if (name === null || name === '') {
        return '.*'
      } else {
        if (name.includes(',')) {
          return '(' + name.replaceAll(',', '|') + ')'
        } else {
          return name
        }
      }
    },
    handleSubmit (name) {
      this.validate(name)
      console.log(this.buildParam)
    },
    submit () {
      this.dataLoading = true
      const param = {
        dbName: this.buildParam.dbName,
        tableName: this.buildParam.tableName,
        srcRegion: this.buildParam.srcRegion,
        dstRegion: this.buildParam.dstRegion,
        filterType: this.buildParam.filterType,
        buName: this.buildParam.buName,
        tps: this.buildParam.tps,
        description: this.buildParam.description,
        disruptionImpact: this.buildParam.disruptionImpact,
        tag: this.buildParam.tag,
        flushExistingData: this.buildParam.flushExistingData,
        orderRelated: this.buildParam.orderRelated,
        gtidInit: this.buildParam.gtidInit,
        remark: this.buildParam.remark,
        applicant: this.buildParam.applicant
      }
      this.axios.post('/api/drc/v2/application/', param).then(res => {
        this.dataLoading = false
        if (res.data.status === 1) {
          this.$Message.error({
            content: '提交失败! ' + res.data.message,
            duration: 2
          })
        } else {
          this.$Message.success('提交成功！')
        }
      })
    },
    validate (name) {
      this.$refs[name].validate((valid) => {
        if (valid) {
          this.submit()
        } else {
          this.$Message.error('仍有必填项未填!')
        }
      })
    },
    handleReset (name) {
      this.$refs[name].resetFields()
    },
    init () {
      this.showDetail = this.$route.query.showDetail
      this.buildParam = {
        dbName: this.$route.query.dbName,
        tableName: this.$route.query.tableName,
        srcRegion: this.$route.query.srcRegion,
        dstRegion: this.$route.query.dstRegion,
        filterType: this.$route.query.filterType,
        buName: this.$route.query.buName,
        tps: this.$route.query.tps,
        description: this.$route.query.description,
        disruptionImpact: this.$route.query.disruptionImpact,
        tag: this.$route.query.tag,
        flushExistingData: this.$route.query.flushExistingData,
        orderRelated: this.$route.query.orderRelated,
        gtidInit: this.$route.query.gtidInit,
        remark: this.$route.query.remark,
        applicant: this.$route.query.applicant
      }
      this.applicationFormId = this.$route.query.applicationFormId
    }
  },
  created () {
    this.getBus()
    this.getRegions()
    this.init()
  }
}
</script>

<style scoped>

</style>
