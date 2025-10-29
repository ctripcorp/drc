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
    value: 'shaali_default',
    label: '上海阿里_default'
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
    value: 'sgpali',
    label: 'sgpali'
  },
  {
    value: 'cftxy',
    label: 'cftxy'
  },
  {
    value: 'cftrb',
    label: 'cftrb'
  },
  {
    value: 'pcixy',
    label: 'pcixy'
  },
  {
    value: 'pcirb',
    label: 'pcirb'
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
  'FRA-AWS-A',
  'FRA-AWS-B',
  'FRA-AWS-C',
  'SHA-ALI-B',
  'SHA-ALI-G',
  'ALI-VPC',
  'NTGXH',
  'SGP-ALI-B',
  'SGP-ALI-C',
  'CFTXY',
  'CFTRB',
  'PCIRB',
  'PCIXY'
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
