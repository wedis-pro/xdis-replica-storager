module github.com/weedge/xdis-replica-storager

go 1.20

require (
	github.com/cloudwego/kitex v0.6.1
	github.com/golang/snappy v0.0.4
	github.com/tidwall/redcon v1.6.2
	github.com/weedge/pkg v0.0.0-20230719122409-952559a3c29e
	github.com/weedge/xdis-standalone v0.0.0-20230720044138-3e851dc7b9eb
	github.com/weedge/xdis-storager v0.0.0-20230719122548-21330058538e
)

require (
	github.com/apache/thrift v0.13.0 // indirect
	github.com/bytedance/gopkg v0.0.0-20230531144706-a12972768317 // indirect
	github.com/choleraehyq/pid v0.0.16 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/google/pprof v0.0.0-20220608213341-c488b8fa1db3 // indirect
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tidwall/btree v1.6.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230530153820-e85fd2cbaebc // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

//replace github.com/tidwall/redcon => ../redcon
replace github.com/tidwall/redcon => github.com/weedge/redcon v0.0.0-20230717070621-d58434c2f821

//replace github.com/weedge/pkg => ../pkg

//replace github.com/weedge/xdis-storager => ../xdis-storager

//replace github.com/weedge/xdis-standalone => ../xdis-standalone
