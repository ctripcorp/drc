<template>
  <div>
    <Form ref="tableInfo" :model="tableInfo" :rules="ruleInline" inline>
      <FormItem prop="namespace">
        <Input type="text" v-model="tableInfo.namespace" @input="namespaceChanged" placeholder="请输入库名（支持正则）"/>
      </FormItem>
      <FormItem prop="name">
        <Input type="text" v-model="tableInfo.name" @input="nameChanged" placeholder="请输入表名（支持正则）">
        </Input>
      </FormItem>
      <FormItem>
        <Button type="primary" @click="handleSubmit('tableInfo')">提交</Button>
      </FormItem>
    </Form>
  </div>
</template>

<script>
export default {
  name: 'tableConfig',
  props: {
    srcMha: String,
    destMha: String,
    applierGroupId: Number,
    srcDc: String,
    destDc: String,
    dataMediaId: Number,
    namespace: String,
    name: String
  },
  data () {
    return {
      tableInfo: {
        namespace: '',
        name: ''
      },
      ruleInline: {
        namespace: [
          { required: true, message: 'Please fill in the schema', trigger: 'blur' }
        ],
        name: [
          { required: true, message: 'Please fill in the table.', trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    handleSubmit () {
      // todo
    },
    submitConfig () {
      // todo
    },
    namespaceChanged () {
      this.$emit('namespaceChange', this.tableInfo.namespace)
    },
    nameChanged () {
      this.$emit('nameChange', this.tableInfo.name)
    }
  }
}
</script>

<style scoped>

</style>
