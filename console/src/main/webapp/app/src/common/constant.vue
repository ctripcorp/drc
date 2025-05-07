<script>
const dcList = [
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
    value: 'ntgxh',
    label: 'ntgxh'
  },
  {
    value: 'fraaws',
    label: 'fraaws'
  },
  {
    value: 'shali',
    label: '上海阿里'
  },
  {
    value: 'sinaws',
    label: 'sinaws'
  },
  {
    value: 'sgpali',
    label: 'sgpali'
  }
]
const tagList = [
  'COMMON',
  'FLT',
  'HTL',
  'OI',
  'TRAVIX',
  'FNC',
  'PCI',
  'TEST'
]
const routeTags = ['meta', 'console']
const azList = [
  'SHARB',
  'SHAXY',
  'SIN-AWS-B',
  'SIN-AWS-C',
  'SIN-VPC',
  'FRA-AWS-A',
  'FRA-AWS-B',
  'FRA-AWS-C',
  'SHA-ALI-B',
  'SHA-ALI-G',
  'ALI-VPC',
  'NTGXH',
  'SGP-ALI-B',
  'SGP-ALI-C'
]
const replicationType = {
  DB_TO_DB: 0,
  DB_TO_MQ: 1,
  DB_TO_KAFKA: 2
}
const mqTypeToReplicationType = new Map([
  ['qmq', 1],
  ['kafka', 2]
])
const mqTypeList = [...mqTypeToReplicationType.keys()]
export default {
  name: 'constant',
  dcList,
  tagList,
  azList,
  mqTypeList,
  replicationType,
  mqTypeToReplicationType,
  routeTags
}
</script>

<style scoped>

</style>
