<template>
  <!--  <Form ref="formValidate" :label-width="100" :model="formValidate" :rules="ruleValidate" inline>-->
  <Form ref="formValidate" :label-width="100" :model="formValidate" inline>
    <br/>
    <br/>
    <span style="font-size: 20px">------------------输入数据一致性全量检验信息---------------------</span>
    <br/>
    <br/>
    <FormItem label="ipA" prop="ipA">
      <Input v-model="formValidate.ipA"/>
    </FormItem>
    <FormItem label="portA" prop="portA">
      <Input v-model="formValidate.portA"/>
    </FormItem>
    <FormItem label="userA" prop="userA">
      <Input v-model="formValidate.userA"/>
    </FormItem>
    <FormItem label="passwordA" prop="passwordA">
      <Input v-model="formValidate.passwordA"/>
    </FormItem>
    <br/>
    <FormItem label="ipB" prop="ipB">
      <Input v-model="formValidate.ipB"/>
    </FormItem>
    <FormItem label="portB" prop="portB">
      <Input v-model="formValidate.portB"/>
    </FormItem>
    <FormItem label="userB" prop="userB">
      <Input v-model="formValidate.userB"/>
    </FormItem>
    <FormItem label="passwordB" prop="passwordB">
      <Input v-model="formValidate.passwordB"/>
    </FormItem>
    <br/>
    <FormItem label="schema" prop="schema">
      <Input v-model="formValidate.schema"/>
    </FormItem>
    <FormItem label="table" prop="table">
      <Input v-model="formValidate.table"/>
    </FormItem>
    <FormItem label="key" prop="key">
      <Input v-model="formValidate.key"/>
    </FormItem>
    <FormItem label="onUpdate" prop="onUpdate">
      <Input v-model="formValidate.onUpdate"/>
    </FormItem>
    <br/>
    <FormItem>
      <Row>
        <Col span="11">
          <FormItem label="startTimestamp" prop="startTimestamp">
            <DatePicker type="datetime" placeholder="Select startTime" formata="yyyy-MM-dd HH:mm:ss"
                        :value="formValidate.startTimestamp"
                        @on-change="formValidate.startTimestamp = $event">
            </DatePicker>
          </FormItem>
        </Col>
        <Col span="11">
          <FormItem label="endTimeStamp" prop="endTimeStamp">
            <DatePicker type="datetime" placeholder="Select endTime" formata="yyyy-MM-dd HH:mm:ss"
                        :value="formValidate.endTimeStamp"
                        @on-change="formValidate.endTimeStamp = $event">
            </DatePicker>
          </FormItem>
        </Col>
      </Row>
    </FormItem>
    <br/>
    <FormItem label=" checkTime
            " prop="checkTime">
      <input id="checkTime" :value="checkTime" placeholder="开始检测的时间" readonly></input>
    </FormItem>
    <Tag id='checkStatus' color="red" style="font-size: 15px">
      数据一致性校验状态未知
    </Tag>
    <br/>
    <FormItem>
      <Button type="primary" @click="handleSubmit('formValidate')">提交校验</Button>
      <Button @click="showCheckStatus()">查看校验状态</Button>
      <Button @click="showResult(formValidate)" style="margin-left: 8px">查看结果</Button>
    </FormItem>
  </Form>
</template>
<script>

import { formatDate } from '../../common/date'

export default {
  // 1.submit 2. checkStatus 3.showResult
  name: 'fullDataConsistencyCheckForTest.vue',
  data () {
    return {
      formValidate: {
        ipA: '',
        portA: '',
        userA: '',
        passwordA: '',
        ipB: '',
        portB: '',
        userB: '',
        passwordB: '',
        schema: '',
        table: '',
        key: 'id',
        onUpdate: 'datachange_lasttime',
        startTimestamp: '',
        endTimeStamp: ''
      },
      checkTime: ''
    }
  },
  methods: {
    handleSubmit (name) {
      // console.log(this.formValidate)
      this.axios.post('/api/drc/v1/monitor/consistency/data/full/internalTest', this.formValidate)
        .then(reponse => {
          if (reponse.data.status === 0) {
            this.$Message.success('成功，开始检验!')
            document.getElementById('checkStatus').textContent = '校验中'
            document.getElementById('checkStatus').color = 'red'
          } else {
            document.getElementById('checkStatus').textContent = '校验中'
            document.getElementById('checkStatus').color = 'red'
            this.$Message.error('失败，已经开始检验!')
          }
        })
    },
    showCheckStatus () {
      this.axios.get('api/drc/v1/monitor/consistency/data/full/checkStatusForTest/' + this.formValidate.schema + '/' + this.formValidate.table + '/' + this.formValidate.key)
        .then(res => {
          if (res.data.status === 0) {
            let checkStatus = '未检验'
            if (res.data.data.checkStatus === 1) checkStatus = '校验中'
            else if (res.data.data.checkStatus === 2) checkStatus = '完成校验'
            else if (res.data.data.checkStatus === 3) checkStatus = '校验失败'
            // const checkStatus = res.data.data.checkStatus === 0 ? '未检验' : (res.data.data.checkStatus === '1' ? '校验中' : (res.data.data.checkStatus === '2' ? '完成校验' : '校验失败'))
            console.log(checkStatus)
            document.getElementById('checkStatus')
              .textContent = checkStatus
            this.checkTime = res.data.data.checkTime
            this.$Message.success('获取状态成功!')
          } else {
            this.$Message.error('获取状态失败!')
          }
        })
    },
    showResult () {
      // router
      this.$router.push({
        name: 'fullDataConsistencyResultTest',
        query: {
          ipA: this.formValidate.ipA,
          portA: this.formValidate.portA,
          userA: this.formValidate.userA,
          passwordA: this.formValidate.passwordA,
          ipB: this.formValidate.ipB,
          portB: this.formValidate.portB,
          userB: this.formValidate.userB,
          passwordB: this.formValidate.passwordB,
          schema: this.formValidate.schema,
          table: this.formValidate.table,
          key: this.formValidate.key,
          checkTime: formatDate(new Date(this.checkTime), 'yyyy-MM-dd hh:mm:ss')
        }
      })
    }
  }
}
</script>

<style scoped>

</style>
