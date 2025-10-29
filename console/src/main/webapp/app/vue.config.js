module.exports = {
  devServer: {
    host: '127.0.0.1',
    port: 8085, // 端口号
    https: false, // https:{type:Boolean}
    open: true, // 配置自动启动浏览器
    before (app) {
      app.use((req, res, next) => {
        if (req.url.startsWith('/api')) {
          req.headers.cookie = 'UBT_VID=1719991383514.5f21JPikmtAG; _RSG=C6qQdT4q6p60yFbKx03kZ8; _RDG=280386659293ae2ba91892ecb9cd733127; _RGUID=6cc6d047-676b-418c-88ee-0c0ed494ff00; _bfaStatusPVSend=1; UAT_CCST_SECRET_ADFC=UAT-7368697278-864e4800a8594464bdbda95922e63437; nfes_isSupportWebP=1; workbench_locale=zh-CN; reportsiteLang=zh-cn; ifscfp=6cc6d047676b418c88ee0c0ed494ff00; Servers_Eid=TR043715; PRO_Servers_Eid=TR043715; GUID=09031093116348758272; FAT_cft_principal=FAT-7368697278-MTc1MzE1ODkxMDE5Mg-6a45003de78f4f209d67fc07848485d2; FAT_Cft_Servers_Eid=TR043715; FAT_cas_principal=FAT-7368697278-MTc1MzE1ODkxMDE3Mw-de0d49c5b80c4cbd9eea760f7237c355-db895bc884360dc4f32c84d7f4c1375b; IFS_R=6cc6d047676b418c88ee0c0ed494ff00; UAT_cas_principal=UAT-7368697278-MTc1NDM5NzA0MzkxMA-72bcccb2661b4d2881dc098853bdc45e-6c3ebf02468a42cefa555a9785110183; IFS_FP=90079A-B0823D-B8C65D; PRO_cas_principal=PRO-7368697278-MTc1NDU3MjM2NDY0Nw-0e4661520c1040bbb5343eb36962771d-2f1df527612485fa696dfef5ef7178a2; offlineTicket=_D78014940269006A06BA8B056776F6D863D8EAF1833619076C74CD2B8C40B9B6; PRO_CCST_SECRET_ADFC=PRO-7368697278-9934d60a6cf3406387ab4b897acab719; _ubtstatus=%7B%22vid%22%3A%221719991383514.5f21JPikmtAG%22%2C%22sid%22%3A1421%2C%22pvid%22%3A6%2C%22pid%22%3A10650009917%7D; _bfi=p1%3D10650009917%26p2%3D10650009917%26v1%3D6%26v2%3D68; _bfaStatus=success; _bfa=1.1719991383514.5f21JPikmtAG.1.1754893788165.1754895854678.1432.1.10650155920; _RF1=10.47.212.46'
        }
        next()
      })
    },
    proxy: {
      '/api': {
        target: 'http://localhost:8080/', // 要访问的接口域名
        ws: true, // 是否启用websockets
        changeOrigin: true // 开启代理：在本地会创建一个虚拟服务端，然后发送请求的数据，并同时接收请求的数据，这样服务端和服务端进行数据的交互就不会有跨域问题
      }
    }
  }
}
