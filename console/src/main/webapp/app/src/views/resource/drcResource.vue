<template>
  <base-component>
    <Breadcrumb :style="{margin: '15px 0 15px 185px', position: 'fixed'}">
      <BreadcrumbItem to="/home">首页</BreadcrumbItem>
      <BreadcrumbItem to="/drcResource"></BreadcrumbItem>
    </Breadcrumb>
    <Content class="content" :style="{padding: '10px', background: '#fff', margin: '50px 0 1px 185px', zIndex: '1'}">
      <div :style="{padding: '1px 1px',height: '100%'}">
        <Row>
          <i-col span="12">
            <Form ref="drcResource" :model="drcResource" :rules="ruleDrcResource" :label-width="250"
                  style="float: left; margin-top: 50px">
              <FormItem label="类型" prop="type">
                <Select v-model="drcResource.type" style="width: 200px" placeholder="请选择资源类型">
                  <Option v-for="item in drcResource.typeList" :value="item.value" :key="item.value">{{ item.label }}
                  </Option>
                </Select>
              </FormItem>
              <FormItem label="dc" prop="dc">
                <Select v-model="drcResource.dc" filterable allow-create style="width: 200px" placeholder="请选择资源所在机房"
                        @on-create="handleCreateDc">
                  <Option v-for="item in drcZoneList" :value="item.value" :key="item.value">{{ item.label }}</Option>
                </Select>
              </FormItem>
              <FormItem label="ip" prop="ip" style="width: 450px">
                <Input v-model="drcResource.ip" placeholder="请输入资源ip"/>
              </FormItem>
              <FormItem label="tag" prop="tag">
                <Select v-model="drcResource.tag" filterable allow-create style="width: 200px" placeholder="选择tag"
                        @on-create="handleCreateTag">
                  <Option v-for="item in tagList" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem label="AZ" prop="az">
                <Select v-model="drcResource.az" filterable allow-create style="width: 200px" placeholder="选择az"
                        @on-create="handleCreateAz">
                  <Option v-for="item in azList" :value="item" :key="item">{{ item }}</Option>
                </Select>
              </FormItem>
              <FormItem>
                <Button @click="handleReset()">重置</Button>
                <br><br>
                <Button type="primary" @click="reviewInputResource ()">录入</Button>
              </FormItem>
              <Modal
                v-model="drcResource.reviewModal"
                title="录入确认"
                @on-ok="inputResource">
                确认录入资源{{ this.drcResource.ip }}吗？所在机房：{{ this.drcResource.dc }}，类型：{{ this.drcResource.type }}
              </Modal>
              <Modal
                v-model="drcResource.resultModal"
                title="录入结果">
                {{ this.result }}
              </Modal>
            </Form>
          </i-col>
        </Row>
      </div>
    </Content>
  </base-component>
</template>

<script>
export default {
  name: 'drcResource',
  data () {
    return {
      status: '',
      title: '',
      message: '',
      hasResp: false,
      drcResource: {
        reviewModal: false,
        ip: '',
        type: '',
        dc: '',
        tag: 'COMMON',
        az: '',
        typeList: [
          {
            value: 'R',
            label: 'Replicator'
          },
          {
            value: 'A',
            label: 'Applier'
          }
        ]
      },
      ruleDrcResource: {
        ip: [
          { required: true, message: 'ip不能为空', trigger: 'blur' }
        ],
        type: [
          { required: true, message: 'ip不能为空', trigger: 'blur' }
        ],
        dc: [
          { required: true, message: 'dc不能为空', trigger: 'blur' }
        ]
      },
      drcZoneList: this.constant.dcList,
      tagList: this.constant.tagList,
      azList: this.constant.azList,
      result: ''
    }
  },
  methods: {
    reviewInputResource () {
      console.log('review input ' + this.drcResource.type + '(' + this.drcResource.ip + ') in ' + this.drcResource.dc)
      this.drcResource.reviewModal = true
    },
    inputResource () {
      const that = this
      this.axios.put('/api/drc/v2/resource/', {
        ip: this.drcResource.ip,
        type: this.drcResource.type,
        dcName: this.drcResource.dc,
        tag: this.drcResource.tag,
        az: this.drcResource.az
      }).then(res => {
        if (res.data.status === 1) {
          this.$Message.warning('录入失败 ' + res.data.message)
          // alert(res.data.message)
        } else {
          that.result = '录入成功'
          that.drcResource.reviewModal = false
          that.drcResource.resultModal = true
        }
      })
    },
    handleCreateDc (val) {
      console.log('customize add dc: ' + val)
      this.drcZoneList.push({
        value: val,
        label: val
      })
      console.log(this.drcZoneList)
    },
    handleCreateTag (val) {
      // this.tagList.push(val)
      this.constant.tagList.push(val)
    },
    handleCreateAz (val) {
      this.constant.azList.push(val)
    },
    handleReset () {
      console.log('reset input request type: ' + this.drcResource.type + ', ip: ' + this.drcResource.ip + ', dc: ' + this.drcResource.dc + '.')
      this.drcResource.ip = ''
      this.drcResource.type = ''
      this.drcResource.dc = ''
      this.drcResource.az = ''
      this.result = ''
      console.log('reset input request type result: ' + this.drcResource.type + ', ip: ' + this.drcResource.ip + ', dc: ' + this.drcResource.dc + '.')
    }
  },
  created () {
  }
}
</script>

<style scoped>

</style>
