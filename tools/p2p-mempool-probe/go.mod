module p2pmempoolprobe

go 1.26.5

require (
	github.com/ethereum/go-ethereum v1.16.0
	github.com/gorilla/websocket v1.5.3
	github.com/klauspost/compress v1.18.1
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/0xPolygon/crand v1.0.3 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/ProjectZKM/Ziren/crates/go-runtime/zkvm_runtime v0.0.0-20260104020744-7268a54d0358 // indirect
	github.com/VictoriaMetrics/fastcache v1.13.0 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/consensys/gnark-crypto v0.19.2 // indirect
	github.com/crate-crypto/go-eth-kzg v1.4.0 // indirect
	github.com/crate-crypto/go-ipa v0.0.0-20240724233137-53bbb0ceb27a // indirect
	github.com/deckarep/golang-set/v2 v2.6.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/ethereum/c-kzg-4844/v2 v2.1.5 // indirect
	github.com/ethereum/go-bigmodexpfix v0.0.0-20250911101455-f9e208c548ab // indirect
	github.com/ethereum/go-verkle v0.2.2 // indirect
	github.com/ferranbt/fastssz v0.1.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/heimdalr/dag v1.4.0 // indirect
	github.com/holiman/bloomfilter/v2 v2.0.3 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/huin/goupnp v1.3.0 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/onsi/gomega v1.36.2 // indirect
	github.com/pion/dtls/v3 v3.0.11 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/stun/v3 v3.1.1 // indirect
	github.com/pion/transport/v4 v4.0.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/supranational/blst v0.3.16 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace (
	cosmossdk.io/client/v2 => github.com/0xPolygon/cosmos-sdk/client/v2 v2.0.0-beta.6
	github.com/Masterminds/goutils => github.com/Masterminds/goutils v1.1.1
	github.com/cometbft/cometbft => github.com/0xPolygon/cometbft v0.3.8-polygon
	github.com/cometbft/cometbft-db => github.com/0xPolygon/cometbft-db v0.14.1-polygon
	github.com/cosmos/cosmos-sdk => github.com/0xPolygon/cosmos-sdk v0.2.13-polygon
	github.com/ethereum/go-ethereum/common/math => github.com/0xPolygon/bor/common/math v1.5.5
	go.mongodb.org/mongo-driver => go.mongodb.org/mongo-driver v1.14.0
)

replace github.com/ethereum/go-ethereum => github.com/0xPolygon/bor v1.14.14-0.20260826150144-7938b8afd281
