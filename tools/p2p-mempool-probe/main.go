// p2p-mempool-probe：实验用 Polygon 轻量 P2P 监听器。
//
// 不同步链、不存状态，只以 eth/68、eth/69 协议连 Bor 节点，收交易广播
// （Transactions / NewPooledTransactionHashes → GetPooledTransactions）和
// 新块广播（NewBlock），同时订阅 rust-uma 的 /uma/v1/ws，按 tx hash 对比：
//
//   - pending_lead = rust-uma 收到该交易事件的时刻 − P2P 首次拿到该交易的时刻
//   - block_lead   = rust-uma 收到该交易事件的时刻 − P2P 首次收到含该交易的块的时刻
//
// 两个时刻都在本机时钟上测，不受跨机时钟偏差影响。
package main

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"math/big"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/dnsdisc"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/nat"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protowire"
)

const networkID = 137

// Bor 官方 sentry 配置模板（packaging/templates/mainnet-v1/sentry/sentry/bor/config.toml）里的
// static-nodes 与 DNS 发现地址；params.BorMainnetBootnodes 那两个 2026-09 已经不回 UDP。
var polygonStatic = []string{
	"enode://48e6326841ce106f6b4e229a1be7e98a1d12be57e328b08cb461f6744ae4e78f5ec2340996ce9b40928a1a90137aadea13e25ca34774b52a3600d13a52c5c7bb@34.185.209.56:30303",
	"enode://8ab6905fe76aa9001adb77135250e918db888cac216870c0e95cf26650d83d31d8c2c93d54c3333e0a2196517c41651d174b743ec3e11f44e595f62b77fec7ba@34.185.162.14:30303",
	"enode://02e0b33cf60fb1f88f853c7c04830156151f4acd1c36173cd3fe1f375801fb4f5be5b3a89c98527915d37ed217752933c3faf4c820df740c9dd681294caebcf6@34.179.171.228:30303",
	"enode://079c387b65b09674825462ea63c528ca996af7b03d19b1b2ab6557347434838067db6dd7ae5e0c2e08d5ba164117f3d7faffbf3e890cb91cffbdf45a433ddfce@35.246.166.189:30303",
	"enode://191d06720948ae0119343e5798098f5b1f95a308174c4119d226da91833bc0176009bcc8bf5012e490500562d4d5b5427c307b01f3485b2e8351ac5afd946864@34.142.28.190:30303",
	"enode://30a4651b245e9a0cec674b9ecb5a06ca01553aa727e14a77d0f1ccdb9e48a975f3be631505f417aae438be545ac3b290cd3ed00bef96efd7fb0fb7f916397b3f@34.39.56.114:30303",
	"enode://b950b98b92e118551d79c7280b97ddfcdf3dacb620367ebd45e8382f8e69390df192055386221025ffd3c03912da2aadf668ae6ea7b35f391d82ef87452b3f02@34.147.169.102:30303",
	"enode://5f6232dc546bf615c7b5bc1c896323340892a1c41097a89a1d38385a5d48bb02f9023377e526911a9da6e4112415aa9f3803cbeeef8243a2bfc4a3d0219ae69e@35.230.142.203:30303",
}

const polygonDNS = "enrtree://AKUEZKN7PSKVNR65FZDHECMKOJQSGPARGTPPBI7WS2VUL4EGR6XPC@pos.polygon-peers.io"

// 已知会发 propose/dispute 的合约：OOv2 本体 + 2026-09-28 mempool 实测里出现的包装合约。
var watchTo = map[common.Address]string{
	common.HexToAddress("0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1"): "oov2",
	common.HexToAddress("0x89e4e7578cb813fd2e9bf0daada9a72fa70aa8b5"): "wrapper_89e4",
	common.HexToAddress("0x94f7ef03ec6b2028bf80facabf8c997bfde36faf"): "wrapper_94f7",
	common.HexToAddress("0x4a1df1565a7704bbfa61e9cd543dd0265cc48bc0"): "wrapper_4a1d",
	// 2026-09-28 P2P 第一轮漏掉的两个（选择器 0x4d02e41e）
	common.HexToAddress("0x7327662f04b14a4777baaae295f364deb16b966f"): "wrapper_7327",
	common.HexToAddress("0x880aaf7dd905d4984ca5b1ee2b44a46e03587cb3"): "wrapper_880a",
}

var selectors = map[string][]byte{
	"proposePrice":    crypto.Keccak256([]byte("proposePrice(address,bytes32,uint256,bytes,int256)"))[:4],
	"proposePriceFor": crypto.Keccak256([]byte("proposePriceFor(address,address,bytes32,uint256,bytes,int256)"))[:4],
	"disputePrice":    crypto.Keccak256([]byte("disputePrice(address,bytes32,uint256,bytes)"))[:4],
	"disputePriceFor": crypto.Keccak256([]byte("disputePriceFor(address,address,bytes32,uint256,bytes)"))[:4],
}

func nowUs() int64 { return time.Now().UnixMicro() }

// peerSeen 是一笔交易在"还不知道它是不是 propose"期间收到的各 peer 公告。
type peerSeen struct {
	peer  string
	delay int64
}

type txInfo struct {
	firstUs    int64 // 任一 peer 首次公告或推送的时刻
	seen       []peerSeen
	announceUs int64 // 首次收到 hash 公告
	fullUs     int64 // 首次拿到完整交易
	via        string
	peer       string
	fetched    bool
	matched    string
}

// peerStat 记录单个 peer 的贡献：谁先送到、晚多少。
type peerStat struct {
	ID, Enode, Name, Addr string
	Version               uint
	ConnectedUs           int64
	ConnectedTotalUs      int64 // 历次连接累计时长（不含当前这次）
	DisconnectedUs        int64
	Connected             bool
	TxAnn, TxFirst        int64
	MatchedAnn            int64
	MatchedFirst          int64
	MatchedFirstGainUs    int64 // 作为第一个送到时，比第二个 peer 早多少（累加）
	BlockAnn, BlockFirst  int64
	txDelays              []int64
	matchedDelays         []int64
	blockDelays           []int64
}

func pushSample(xs *[]int64, v int64) {
	if len(*xs) < 4000 {
		*xs = append(*xs, v)
		return
	}
	(*xs)[rand.IntN(len(*xs))] = v
}

func quant(xs []int64, q float64) any {
	if len(xs) == 0 {
		return nil
	}
	c := slices.Clone(xs)
	slices.Sort(c)
	return float64(c[int(float64(len(c)-1)*q+0.5)]) / 1000
}

type blockAnn struct {
	hash   common.Hash
	number uint64
	atUs   int64
}

type umaInfo struct {
	recvUs     int64
	upstreamUs int64
	eventType  uint64
	pendingUs  int64
	blockUs    int64
	matched    string
}

type probe struct {
	mu           sync.Mutex
	txs          map[common.Hash]*txInfo
	blockTx      map[common.Hash]int64 // tx hash -> 首次收到含它的块
	blocks       map[common.Hash]bool
	blockEmitted map[common.Hash]bool
	bodyReq      map[uint64]blockAnn // GetBlockBodies 请求 id -> 块公告
	uma          map[common.Hash]*umaInfo
	peers        map[string]*peerStat
	blockSeen    map[common.Hash]int64 // 块 hash -> 首次公告时刻（按 peer 算块延迟）
	blockPeer    map[common.Hash]string
	hdrReq       map[uint64]common.Hash // GetBlockHeaders 请求 id -> 块 hash
	rcptReq      map[uint64]common.Hash
	borCfg       *params.BorConfig
	deny         map[string]bool
	seats        *seatManager
	out          *os.File
	observer     string
	push         chan []byte
	feedSubs     map[chan []byte]struct{}
	signer       types.Signer
	genesis      common.Hash
	td           *big.Int
	forkID       forkid.ID
	staticFlt    forkid.Filter

	// 统计
	peersByVer    map[uint]int
	msgCodes      map[string]int
	decodeErrs    map[string]int
	handshakeFail map[string]int
	announced     int64
	fullTxs       int64
	pushTxs       int64
	newBlocks     int64
	matchedTxs    int64
	umaEvents     int64
	umaFrames     int64
	windowFull    int64
}

func (p *probe) emit(v map[string]any) {
	p.mu.Lock()
	p.emitLocked(v)
	p.mu.Unlock()
}

// emitLocked 在已持锁时写。
func (p *probe) emitLocked(v map[string]any) {
	if p.observer != "" {
		v["obs"] = p.observer
	}
	b, _ := json.Marshal(v)
	b = append(b, '\n')
	p.out.Write(b)
	if len(p.feedSubs) > 0 && v["kind"] == "p2p_match" {
		for ch := range p.feedSubs {
			select {
			case ch <- b:
			default: // 订阅方读得慢就丢，不拖慢收包
			}
		}
	}
	if p.push != nil {
		select {
		case p.push <- b:
		default: // 推送通道满了宁可丢，不拖慢收包
		}
	}
}

// runFeed 在本机端口上把 p2p_match 行实时推给订阅方（mempool-uma）。
func (p *probe) runFeed(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("feed listen: %v", err)
	}
	log.Printf("pending feed listening on %s", addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		ch := make(chan []byte, 10000)
		p.mu.Lock()
		p.feedSubs[ch] = struct{}{}
		p.mu.Unlock()
		go func() {
			defer func() {
				p.mu.Lock()
				delete(p.feedSubs, ch)
				p.mu.Unlock()
				c.Close()
			}()
			for b := range ch {
				c.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := c.Write(b); err != nil {
					return
				}
			}
		}()
	}
}

// runPush 把事件行按秒打包 POST 给汇总端（三角定位用）。汇总端挂了只丢这批，不影响本地记录。
func (p *probe) runPush(url string) {
	client := &http.Client{Timeout: 10 * time.Second}
	var buf bytes.Buffer
	tick := time.NewTicker(time.Second)
	for {
		select {
		case line := <-p.push:
			buf.Write(line)
			if buf.Len() < 256*1024 {
				continue
			}
		case <-tick.C:
		}
		if buf.Len() == 0 {
			continue
		}
		resp, err := client.Post(url, "application/x-ndjson", bytes.NewReader(buf.Bytes()))
		if err != nil {
			log.Printf("push: %v", err)
		} else {
			resp.Body.Close()
		}
		buf.Reset()
	}
}

// runRTT 每 10 分钟测一次到各已连接 peer 的 TCP 建连耗时（≈ RTT），给汇总端做"地区→观测点"延迟标定。
func (p *probe) runRTT() {
	for {
		time.Sleep(time.Minute)
		p.mu.Lock()
		addrs := map[string]string{}
		for id, ps := range p.peers {
			if ps.Connected {
				addrs[id] = ps.Addr
			}
		}
		p.mu.Unlock()
		for id, addr := range addrs {
			start := time.Now()
			c, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				continue
			}
			rtt := time.Since(start)
			c.Close()
			p.emit(map[string]any{"kind": "rtt", "peer": id, "addr": addr, "rtt_ms": float64(rtt.Microseconds()) / 1000})
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(9 * time.Minute)
	}
}

func classify(tx *types.Transaction) string {
	if to := tx.To(); to != nil {
		if name, ok := watchTo[*to]; ok {
			return name
		}
	}
	data := tx.Data()
	for name, sel := range selectors {
		if bytes.HasPrefix(data, sel) {
			return "direct:" + name
		}
	}
	for name, sel := range selectors {
		if bytes.Contains(data, sel) {
			return "embedded:" + name
		}
	}
	return ""
}

// noteTx 需持锁调用：记录 peer 送到某笔交易（公告或推送）的时刻，更新 peer 贡献。
func (p *probe) noteTx(peer string, info *txInfo, at int64, isNew bool) {
	ps := p.peers[peer]
	if ps == nil {
		return
	}
	ps.TxAnn++
	var delay int64
	// 刚连上的 1 分钟内对方会把整个交易池公告一遍，那些不是实时传播，不计延迟也不算首达。
	warm := at-ps.ConnectedUs < 60_000_000
	if isNew {
		if !warm {
			ps.TxFirst++
		}
	} else {
		delay = at - info.firstUs
		if !warm {
			pushSample(&ps.txDelays, delay)
		}
	}
	switch {
	case info.matched != "":
		p.creditMatched(peer, info, delay)
	case info.fullUs == 0:
		info.seen = append(info.seen, peerSeen{peer, delay})
	}
}

// creditMatched 需持锁调用。
func (p *probe) creditMatched(peer string, info *txInfo, delay int64) {
	ps := p.peers[peer]
	if ps == nil {
		return
	}
	ps.MatchedAnn++
	pushSample(&ps.matchedDelays, delay)
	if delay == 0 {
		ps.MatchedFirst++
	}
}

// onAnnounce 记录 hash 公告，返回需要向该 peer 拉取的 hash。
func (p *probe) onAnnounce(peer string, pkt *eth.NewPooledTransactionHashesPacket) []common.Hash {
	at := nowUs()
	var fetch []common.Hash
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, h := range pkt.Hashes {
		p.announced++
		info := p.txs[h]
		isNew := info == nil
		if isNew {
			info = &txInfo{announceUs: at, firstUs: at, peer: peer}
			p.txs[h] = info
		}
		p.noteTx(peer, info, at, isNew)
		if info.fetched || info.fullUs != 0 {
			continue
		}
		if i < len(pkt.Types) && pkt.Types[i] == types.BlobTxType {
			continue
		}
		if i < len(pkt.Sizes) && pkt.Sizes[i] > 256*1024 {
			continue
		}
		info.fetched = true
		fetch = append(fetch, h)
	}
	return fetch
}

func (p *probe) onTx(peer string, tx *types.Transaction, via string) {
	at := nowUs()
	h := tx.Hash()
	p.mu.Lock()
	defer p.mu.Unlock()
	if via == "push" {
		p.pushTxs++
	}
	info := p.txs[h]
	isNew := info == nil
	if isNew {
		info = &txInfo{announceUs: at, firstUs: at, peer: peer}
		p.txs[h] = info
	}
	if via == "push" {
		p.noteTx(peer, info, at, isNew)
	}
	if info.fullUs != 0 {
		return
	}
	info.fullUs = at
	info.via = via
	p.fullTxs++
	p.windowFull++
	m := classify(tx)
	if m == "" {
		info.seen = nil
		return
	}
	info.matched = m
	p.matchedTxs++
	// 把"知道它是 propose 之前"收到的公告补记到各 peer 的 propose 贡献里。
	var second int64 = -1
	for _, s := range info.seen {
		p.creditMatched(s.peer, info, s.delay)
		if s.delay > 0 && (second < 0 || s.delay < second) {
			second = s.delay
		}
	}
	if len(info.seen) > 0 && info.seen[0].delay == 0 && second > 0 {
		if ps := p.peers[info.seen[0].peer]; ps != nil {
			ps.MatchedFirstGainUs += second
		}
	}
	p.seatCreditTx(info.seen, second)
	info.seen = nil
	from, _ := types.Sender(p.signer, tx)
	to := ""
	if tx.To() != nil {
		to = strings.ToLower(tx.To().Hex())
	}
	sel := ""
	if len(tx.Data()) >= 4 {
		sel = hex.EncodeToString(tx.Data()[:4])
	}
	rec := map[string]any{
		"kind": "p2p_match", "hash": h.Hex(), "match": m, "to": to, "from": strings.ToLower(from.Hex()),
		"selector": sel, "announce_us": info.announceUs, "full_us": at, "via": via, "peer": info.peer, "first_us": info.firstUs,
		"nonce": tx.Nonce(), "tip_gwei": new(big.Int).Div(tx.GasTipCap(), big.NewInt(1e9)).Int64(),
		"input": hex.EncodeToString(tx.Data()), // 完整调用数据：给提交地址打分时核对报价
	}
	if u := p.uma[h]; u != nil {
		// rust-uma 比 P2P 还早：记负领先。
		u.pendingUs = at
		rec["late_vs_uma_ms"] = float64(at-u.recvUs) / 1000
	}
	p.emitLocked(rec)
}

func (p *probe) onBlock(peer string, block *types.Block) {
	p.mu.Lock()
	defer p.mu.Unlock()
	at := nowUs()
	p.noteBlock(peer, block.Hash(), at)
	if p.blocks[block.Hash()] {
		return
	}
	p.blocks[block.Hash()] = true
	p.recordBlockTxs(peer, block.NumberU64(), at, block.Transactions())
}

// onBlockHashes 记录块公告时刻，返回需要向该 peer 拉块体的新块。
func (p *probe) noteBlock(peer string, hash common.Hash, at int64) {
	ps := p.peers[peer]
	first, ok := p.blockSeen[hash]
	if !ok {
		p.blockSeen[hash] = at
		p.blockPeer[hash] = peer
	}
	p.seatCreditBlock(peer, hash, !ok, at-first)
	if ps == nil {
		return
	}
	ps.BlockAnn++
	if !ok {
		ps.BlockFirst++
	} else {
		pushSample(&ps.blockDelays, at-first)
	}
}

// sealHash 同 bor.SealHash（consensus/bor/bor.go）。不直接引那个包：它会拖进只在 cgo 下能编的 cosmos-sdk。
func sealHash(h *types.Header, c *params.BorConfig) common.Hash {
	enc := []any{
		h.ParentHash, h.UncleHash, h.Coinbase, h.Root, h.TxHash, h.ReceiptHash, h.Bloom,
		h.Difficulty, h.Number, h.GasLimit, h.GasUsed, h.Time,
		h.Extra[:len(h.Extra)-types.ExtraSealLength], h.MixDigest, h.Nonce,
	}
	if c.IsJaipur(h.Number) && h.BaseFee != nil {
		enc = append(enc, h.BaseFee)
	}
	b, _ := rlp.EncodeToBytes(enc)
	return crypto.Keccak256Hash(b)
}

// author 从 Bor 块头 extraData 末尾的 65 字节签名恢复出块者（与 bor_getAuthor 相同）。
func (p *probe) author(h *types.Header) string {
	if len(h.Extra) < types.ExtraSealLength {
		return ""
	}
	pub, err := crypto.Ecrecover(sealHash(h, p.borCfg).Bytes(), h.Extra[len(h.Extra)-types.ExtraSealLength:])
	if err != nil {
		return ""
	}
	return strings.ToLower(common.BytesToAddress(crypto.Keccak256(pub[1:])[12:]).Hex())
}

// emitBlockLocked 需持锁调用：每个块只输出一次"块 + 出块者 + 首个公告 peer"。
func (p *probe) emitBlockLocked(h *types.Header) {
	hash := h.Hash()
	if p.blockEmitted[hash] {
		return
	}
	p.blockEmitted[hash] = true
	p.emitLocked(map[string]any{
		"kind": "block", "hash": hash.Hex(), "number": h.Number.Uint64(), "author": p.author(h),
		"first_peer": p.blockPeer[hash], "first_us": p.blockSeen[hash], "header_time": h.Time,
		"txs_hint": h.GasUsed,
	})
}

func (p *probe) onHeaders(pkt *eth.BlockHeadersPacket) {
	p.mu.Lock()
	defer p.mu.Unlock()
	want, ok := p.hdrReq[pkt.RequestId]
	if !ok {
		return
	}
	delete(p.hdrReq, pkt.RequestId)
	for _, h := range pkt.BlockHeadersRequest {
		if h.Hash() == want {
			p.emitBlockLocked(h)
		}
	}
}

func (p *probe) onBlockHashes(peer string, pkt *eth.NewBlockHashesPacket) (uint64, []common.Hash) {
	at := nowUs()
	p.mu.Lock()
	defer p.mu.Unlock()
	var fetch []common.Hash
	id := rand.Uint64()
	for _, b := range *pkt {
		p.noteBlock(peer, b.Hash, at)
	}
	for _, b := range *pkt {
		if p.blocks[b.Hash] {
			continue
		}
		p.blocks[b.Hash] = true
		p.bodyReq[id] = blockAnn{hash: b.Hash, number: b.Number, atUs: at}
		fetch = append(fetch, b.Hash)
		break // 一次只拉一个块，请求 id 与块一一对应
	}
	return id, fetch
}

func (p *probe) onBodies(peer string, pkt *eth.BlockBodiesPacket) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ann, ok := p.bodyReq[pkt.RequestId]
	if !ok {
		return
	}
	delete(p.bodyReq, pkt.RequestId)
	if len(pkt.BlockBodiesResponse) == 0 {
		// 对方没给，允许下一个公告者再拉。
		delete(p.blocks, ann.hash)
		return
	}
	p.recordBlockTxs(peer, ann.number, ann.atUs, pkt.BlockBodiesResponse[0].Transactions)
}

// recordBlockTxs 需持锁调用；at 取块首次公告的时刻。
func (p *probe) recordBlockTxs(peer string, number uint64, at int64, txs []*types.Transaction) {
	p.newBlocks++
	for _, tx := range txs {
		h := tx.Hash()
		if _, ok := p.blockTx[h]; ok {
			continue
		}
		p.blockTx[h] = at
		if m := classify(tx); m != "" {
			// 进块的 propose 类交易（含 mempool 里从没见过的私有交易）：发送方、路由、是否公开。
			from, _ := types.Sender(p.signer, tx)
			rec := map[string]any{
				"kind": "incl", "hash": h.Hex(), "number": number, "block_us": at, "match": m,
				"from": strings.ToLower(from.Hex()), "to": strings.ToLower(tx.To().Hex()),
				"tip_gwei": new(big.Int).Div(tx.GasTipCap(), big.NewInt(1e9)).Int64(),
				"nonce":    tx.Nonce(), "input": hex.EncodeToString(tx.Data()),
			}
			if info := p.txs[h]; info != nil && info.fullUs != 0 {
				rec["in_mempool"], rec["first_peer"], rec["first_us"] = true, info.peer, info.firstUs
			} else {
				rec["in_mempool"] = false
			}
			p.emitLocked(rec)
		}
		if u := p.uma[h]; u != nil && u.blockUs == 0 {
			u.blockUs = at
			p.emitLocked(map[string]any{
				"kind": "block_after_uma", "hash": h.Hex(), "block": number,
				"block_us": at, "uma_recv_us": u.recvUs, "block_lead_ms": float64(u.recvUs-at) / 1000, "peer": peer,
			})
		}
	}
}

func (p *probe) onUma(h common.Hash, e umaEv, recvUs int64) {
	eventType, upstreamUs := e.eventType, e.upstream
	p.mu.Lock()
	defer p.mu.Unlock()
	p.umaEvents++
	if _, ok := p.uma[h]; ok {
		return
	}
	u := &umaInfo{recvUs: recvUs, upstreamUs: int64(upstreamUs), eventType: eventType}
	rec := map[string]any{
		"kind": "uma", "hash": h.Hex(), "event_type": eventType, "uma_recv_us": recvUs,
		"uma_upstream_us": upstreamUs, "market_id": e.marketID, "category": e.category,
		"bet_type": e.betType, "outcome": e.outcome, "neg_risk": e.negRisk,
	}
	if info := p.txs[h]; info != nil {
		first := info.fullUs
		if first == 0 || (info.announceUs != 0 && info.announceUs < first) {
			first = info.announceUs
		}
		u.pendingUs = first
		u.matched = info.matched
		rec["p2p_first_us"] = first
		rec["p2p_full_us"] = info.fullUs
		rec["pending_lead_ms"] = float64(recvUs-first) / 1000
		rec["match"] = info.matched
		rec["via"] = info.via
	}
	if b, ok := p.blockTx[h]; ok {
		u.blockUs = b
		rec["block_us"] = b
		rec["block_lead_ms"] = float64(recvUs-b) / 1000
	}
	p.uma[h] = u
	p.emitLocked(rec)
}

func pct(xs []float64, q float64) any {
	if len(xs) == 0 {
		return nil
	}
	slices.Sort(xs)
	return xs[int(float64(len(xs)-1)*q+0.5)]
}

func (p *probe) summary(srv *p2p.Server, window time.Duration) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	var pend, blk []float64
	withPending, withBlock := 0, 0
	for _, u := range p.uma {
		if u.pendingUs != 0 {
			withPending++
			pend = append(pend, float64(u.recvUs-u.pendingUs)/1000)
		}
		if u.blockUs != 0 {
			withBlock++
			blk = append(blk, float64(u.recvUs-u.blockUs)/1000)
		}
	}
	vers := map[string]int{}
	for v, n := range p.peersByVer {
		vers[fmt.Sprintf("eth%d", v)] = n
	}
	s := map[string]any{
		"kind": "summary", "at_us": nowUs(), "peers": srv.PeerCount(), "peers_by_version": vers,
		"handshake_fail": maps.Clone(p.handshakeFail), "msg_codes": maps.Clone(p.msgCodes), "decode_errs": maps.Clone(p.decodeErrs), "announced": p.announced, "full_txs": p.fullTxs,
		"push_txs": p.pushTxs, "full_tx_per_sec": float64(p.windowFull) / window.Seconds(),
		"new_blocks": p.newBlocks, "p2p_matched": p.matchedTxs, "uma_frames": p.umaFrames,
		"uma_events": p.umaEvents, "uma_txs": len(p.uma), "uma_with_pending": withPending,
		"uma_with_block":      withBlock,
		"pending_lead_ms_p10": pct(pend, 0.1), "pending_lead_ms_p50": pct(pend, 0.5),
		"pending_lead_ms_p90": pct(pend, 0.9),
		"block_lead_ms_p10":   pct(blk, 0.1), "block_lead_ms_p50": pct(blk, 0.5), "block_lead_ms_p90": pct(blk, 0.9),
	}
	p.windowFull = 0
	return s
}

// dumpPeers 覆盖写 peers.json：每个 peer 的贡献，供评估和剔除。
func (p *probe) dumpPeers(path string) {
	p.mu.Lock()
	var rows []map[string]any
	for _, ps := range p.peers {
		st, a, b, c := "", 0.0, 0.0, 0.0
		if s := p.seats.seats[ps.ID]; s != nil {
			p.seats.flush(s)
			st = s.State
			if s.online && s.explore {
				st = "explore"
			}
			a, b, c = s.shares()
		}
		rows = append(rows, map[string]any{
			"state": st, "share_a": a, "share_b": b, "share_c": c,
			"id": ps.ID, "enode": ps.Enode, "name": ps.Name, "addr": ps.Addr, "version": ps.Version,
			"connected": ps.Connected, "connected_us": ps.ConnectedUs, "disconnected_us": ps.DisconnectedUs,
			"connected_minutes": float64(ps.ConnectedTotalUs+map[bool]int64{true: nowUs() - ps.ConnectedUs, false: 0}[ps.Connected]) / 60e6,
			"tx_ann":            ps.TxAnn, "tx_first": ps.TxFirst,
			"tx_delay_ms_p50": quant(ps.txDelays, 0.5), "tx_delay_ms_p90": quant(ps.txDelays, 0.9),
			"matched_ann": ps.MatchedAnn, "matched_first": ps.MatchedFirst,
			"matched_first_gain_ms": float64(ps.MatchedFirstGainUs) / 1000,
			"matched_delay_ms_p50":  quant(ps.matchedDelays, 0.5), "matched_delay_ms_p90": quant(ps.matchedDelays, 0.9),
			"block_ann": ps.BlockAnn, "block_first": ps.BlockFirst,
			"block_delay_ms_p50": quant(ps.blockDelays, 0.5), "block_delay_ms_p90": quant(ps.blockDelays, 0.9),
		})
	}
	for id, s := range p.seats.seats { // 本次进程没连上过的（冷却、失联核心等）也列出来
		if _, ok := p.peers[id]; ok || s.State == stCandidate {
			continue
		}
		a, b, c := s.shares()
		rows = append(rows, map[string]any{"id": id, "enode": s.Enode, "addr": s.Addr, "name": s.Name, "state": s.State,
			"share_a": a, "share_b": b, "share_c": c, "connected": false, "reason": s.LastReason, "cooldown_until_us": s.CooldownUntil})
	}
	p.seats.saveLocked()
	p.mu.Unlock()
	b, _ := json.MarshalIndent(rows, "", " ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		os.Rename(tmp, path)
	}
}

func (p *probe) prune() {
	cut := nowUs() - int64(15*time.Minute/time.Microsecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	for h, info := range p.txs {
		if info.firstUs < cut {
			delete(p.txs, h)
		}
	}
	for h, t := range p.blockSeen {
		if t < cut {
			delete(p.blockSeen, h)
			delete(p.blockPeer, h)
			delete(p.blockEmitted, h)
		}
	}
	if len(p.hdrReq) > 10000 {
		p.hdrReq = map[uint64]common.Hash{}
	}
	if len(p.rcptReq) > 10000 {
		p.rcptReq = map[uint64]common.Hash{}
	}
	for h, t := range p.blockTx {
		if t < cut {
			delete(p.blockTx, h)
		}
	}
	if len(p.blocks) > 20000 {
		p.blocks = map[common.Hash]bool{}
	}
	for id, ann := range p.bodyReq {
		if ann.atUs < cut {
			delete(p.bodyReq, id)
		}
	}
}

func (p *probe) failed(reason string) {
	p.mu.Lock()
	p.handshakeFail[reason]++
	p.mu.Unlock()
}

// ---- eth 协议 ----

func (p *probe) handshake(rw p2p.MsgReadWriter, version uint) error {
	errc := make(chan error, 2)
	go func() {
		if version == eth.ETH69 {
			errc <- p2p.Send(rw, eth.StatusMsg, &eth.StatusPacket69{
				ProtocolVersion: uint32(version), NetworkID: networkID, TD: p.td,
				Genesis: p.genesis, ForkID: p.forkID,
				EarliestBlock: 0, LatestBlock: 0, LatestBlockHash: p.genesis,
			})
			return
		}
		errc <- p2p.Send(rw, eth.StatusMsg, &eth.StatusPacket68{
			ProtocolVersion: uint32(version), NetworkID: networkID, TD: p.td,
			Head: p.genesis, Genesis: p.genesis, ForkID: p.forkID,
		})
	}()
	go func() {
		msg, err := rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		defer msg.Discard()
		if msg.Code != eth.StatusMsg {
			errc <- fmt.Errorf("first msg code %d", msg.Code)
			return
		}
		var nid uint64
		var genesis common.Hash
		if version == eth.ETH69 {
			var st eth.StatusPacket69
			if err := msg.Decode(&st); err != nil {
				errc <- fmt.Errorf("decode status: %w", err)
				return
			}
			nid, genesis = st.NetworkID, st.Genesis
		} else {
			var st eth.StatusPacket68
			if err := msg.Decode(&st); err != nil {
				errc <- fmt.Errorf("decode status: %w", err)
				return
			}
			nid, genesis = st.NetworkID, st.Genesis
		}
		if nid != networkID {
			errc <- fmt.Errorf("network %d", nid)
			return
		}
		if genesis != p.genesis {
			errc <- fmt.Errorf("genesis mismatch")
			return
		}
		errc <- nil
	}()
	timeout := time.After(8 * time.Second)
	for range 2 {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
		case <-timeout:
			return fmt.Errorf("handshake timeout")
		}
	}
	return nil
}

func (p *probe) run(peer *p2p.Peer, rw p2p.MsgReadWriter, version uint) error {
	full := peer.ID().String()
	id := full[:16]
	p.mu.Lock()
	ps := p.peers[id]
	if ps == nil {
		ps = &peerStat{ID: id}
		p.peers[id] = ps
	}
	if err := p.seatAdmit(id, peer, ps); err != nil {
		p.mu.Unlock()
		p.failed("seat:" + err.Error())
		return err
	}
	p.mu.Unlock()
	if err := p.handshake(rw, version); err != nil {
		reason := err.Error()
		if strings.HasPrefix(reason, "network") {
			reason = "network_mismatch"
		}
		p.failed(reason)
		p.mu.Lock()
		p.seatLeave(id)
		p.mu.Unlock()
		return err
	}
	p.mu.Lock()
	p.peersByVer[version]++
	ps.Enode, ps.Name, ps.Addr, ps.Version = peer.Node().URLv4(), peer.Fullname(), peer.RemoteAddr().String(), version
	ps.Connected, ps.ConnectedUs, ps.DisconnectedUs = true, nowUs(), 0
	p.emitLocked(map[string]any{"kind": "peer", "id": id, "addr": ps.Addr, "name": ps.Name, "enode": ps.Enode})
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.peersByVer[version]--
		p.seatLeave(id)
		ps.Connected, ps.DisconnectedUs = false, nowUs()
		ps.ConnectedTotalUs += ps.DisconnectedUs - ps.ConnectedUs
		p.mu.Unlock()
	}()
	for {
		msg, err := rw.ReadMsg()
		if err != nil {
			return err
		}
		if err := p.handle(id, rw, msg); err != nil {
			msg.Discard()
			return err
		}
		msg.Discard()
	}
}

type emptyResp struct {
	RequestId uint64
	List      []rlp.RawValue
}

func (p *probe) decodeErr(code uint64, err error) {
	p.mu.Lock()
	k := fmt.Sprintf("%d:%v", code, err)
	if len(k) > 120 {
		k = k[:120]
	}
	p.decodeErrs[k]++
	p.mu.Unlock()
}

func (p *probe) handle(peer string, rw p2p.MsgReadWriter, msg p2p.Msg) error {
	p.mu.Lock()
	p.msgCodes[fmt.Sprintf("0x%02x", msg.Code)]++
	p.mu.Unlock()
	switch msg.Code {
	case eth.TransactionsMsg:
		var txs eth.TransactionsPacket
		if err := msg.Decode(&txs); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		for _, tx := range txs {
			p.onTx(peer, tx, "push")
		}
	case eth.NewPooledTransactionHashesMsg:
		var ann eth.NewPooledTransactionHashesPacket
		if err := msg.Decode(&ann); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		fetch := p.onAnnounce(peer, &ann)
		for len(fetch) > 0 {
			n := min(len(fetch), 256)
			req := &eth.GetPooledTransactionsPacket{RequestId: rand.Uint64(), GetPooledTransactionsRequest: fetch[:n]}
			if err := p2p.Send(rw, eth.GetPooledTransactionsMsg, req); err != nil {
				return err
			}
			fetch = fetch[n:]
		}
	case eth.PooledTransactionsMsg:
		var resp eth.PooledTransactionsPacket
		if err := msg.Decode(&resp); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		for _, tx := range resp.PooledTransactionsResponse {
			p.onTx(peer, tx, "fetch")
		}
	case eth.NewBlockHashesMsg:
		var nbh eth.NewBlockHashesPacket
		if err := msg.Decode(&nbh); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		if id, fetch := p.onBlockHashes(peer, &nbh); len(fetch) > 0 {
			hid := rand.Uint64()
			p.mu.Lock()
			p.hdrReq[hid] = fetch[0]
			p.mu.Unlock()
			if err := p2p.Send(rw, eth.GetBlockHeadersMsg, &eth.GetBlockHeadersPacket{RequestId: hid,
				GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{Origin: eth.HashOrNumber{Hash: fetch[0]}, Amount: 1}}); err != nil {
				return err
			}
			// 顺带向同一个 peer 要收据：收据里就有 log，轻量节点不执行交易也能直接拿到 ProposePrice 事件。
			rid := rand.Uint64()
			p.mu.Lock()
			p.rcptReq[rid] = fetch[0]
			p.mu.Unlock()
			if err := p2p.Send(rw, eth.GetReceiptsMsg, &eth.GetReceiptsPacket{RequestId: rid, GetReceiptsRequest: fetch}); err != nil {
				return err
			}
			return p2p.Send(rw, eth.GetBlockBodiesMsg, &eth.GetBlockBodiesPacket{RequestId: id, GetBlockBodiesRequest: fetch})
		}
	case eth.BlockHeadersMsg:
		at := nowUs()
		var bh eth.BlockHeadersPacket
		if err := msg.Decode(&bh); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		p.mu.Lock()
		if h, ok := p.hdrReq[bh.RequestId]; ok {
			p.emitLocked(map[string]any{"kind": "bt", "ev": "hdr", "hash": h.Hex(), "us": at, "ok": len(bh.BlockHeadersRequest) > 0, "peer": peer})
		}
		p.mu.Unlock()
		p.onHeaders(&bh)
	case eth.BlockBodiesMsg:
		at := nowUs()
		var bb eth.BlockBodiesPacket
		if err := msg.Decode(&bb); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		p.mu.Lock()
		if ann, ok := p.bodyReq[bb.RequestId]; ok {
			p.emitLocked(map[string]any{"kind": "bt", "ev": "body", "hash": ann.hash.Hex(), "us": at, "ok": len(bb.BlockBodiesResponse) > 0, "peer": peer})
		}
		p.mu.Unlock()
		p.onBodies(peer, &bb)
	case eth.ReceiptsMsg:
		at := nowUs()
		var rr eth.ReceiptsRLPPacket
		if err := msg.Decode(&rr); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		p.mu.Lock()
		if h, ok := p.rcptReq[rr.RequestId]; ok {
			delete(p.rcptReq, rr.RequestId)
			ok2 := len(rr.ReceiptsRLPResponse) > 0 && len(rr.ReceiptsRLPResponse[0]) > 2
			p.emitLocked(map[string]any{"kind": "bt", "ev": "rcpt", "hash": h.Hex(), "us": at, "ok": ok2, "peer": peer,
				"bytes": func() int {
					if len(rr.ReceiptsRLPResponse) > 0 {
						return len(rr.ReceiptsRLPResponse[0])
					}
					return 0
				}()})
		}
		p.mu.Unlock()
	case eth.NewBlockMsg:
		var nb eth.NewBlockPacket
		if err := msg.Decode(&nb); err != nil {
			p.decodeErr(msg.Code, err)
			return nil
		}
		p.onBlock(peer, nb.Block)
		p.mu.Lock()
		p.emitBlockLocked(nb.Block.Header())
		p.mu.Unlock()
	case eth.GetBlockHeadersMsg, eth.GetBlockBodiesMsg, eth.GetReceiptsMsg, eth.GetPooledTransactionsMsg:
		// 我们没有链数据，按协议回一个空响应，避免被当成不响应的 peer 断开。
		var id struct {
			RequestId uint64
			Rest      []rlp.RawValue `rlp:"tail"`
		}
		if err := msg.Decode(&id); err != nil {
			return nil
		}
		code := map[uint64]uint64{
			eth.GetBlockHeadersMsg: eth.BlockHeadersMsg, eth.GetBlockBodiesMsg: eth.BlockBodiesMsg,
			eth.GetReceiptsMsg: eth.ReceiptsMsg, eth.GetPooledTransactionsMsg: eth.PooledTransactionsMsg,
		}[msg.Code]
		return p2p.Send(rw, code, &emptyResp{RequestId: id.RequestId})
	}
	return nil
}

// ---- 发现：过滤掉 ENR 里 eth 字段不是 Polygon 的节点 ----

type ethEntry struct {
	ForkID forkid.ID
	Rest   []rlp.RawValue `rlp:"tail"`
}

func (ethEntry) ENRKey() string { return "eth" }

type lazyIter struct {
	srv   *p2p.Server
	inner enode.Iterator
	flt   func(*enode.Node) bool
	mu    sync.Mutex
}

func (it *lazyIter) get() enode.Iterator {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.inner == nil {
		for it.srv.DiscoveryV4() == nil {
			time.Sleep(100 * time.Millisecond)
		}
		it.inner = enode.Filter(it.srv.DiscoveryV4().RandomNodes(), it.flt)
	}
	return it.inner
}
func (it *lazyIter) Next() bool        { return it.get().Next() }
func (it *lazyIter) Node() *enode.Node { return it.get().Node() }
func (it *lazyIter) Close()            { it.get().Close() }

// ---- rust-uma 订阅 ----

func (p *probe) runUma(url string) {
	dec, _ := zstd.NewReader(nil)
	dialer := websocket.Dialer{Subprotocols: []string{"uma.pb.v1"}, HandshakeTimeout: 10 * time.Second}
	for {
		conn, _, err := dialer.Dial(url, nil)
		if err != nil {
			log.Printf("rust-uma dial: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("rust-uma subscribed %s", url)
		for {
			kind, data, err := conn.ReadMessage()
			recv := nowUs()
			if err != nil {
				log.Printf("rust-uma read: %v", err)
				break
			}
			if kind != websocket.BinaryMessage || len(data) < 12 || string(data[:4]) != "UMA1" {
				continue
			}
			p.mu.Lock()
			p.umaFrames++
			p.mu.Unlock()
			body := data[12:]
			if data[4]&1 != 0 {
				size := binary.BigEndian.Uint32(data[8:12])
				out, err := dec.DecodeAll(body, make([]byte, 0, size))
				if err != nil {
					continue
				}
				body = out
			}
			walkBatch(body, func(h common.Hash, e umaEv) { p.onUma(h, e, recv) })
		}
		conn.Close()
		time.Sleep(time.Second)
	}
}

func walkBatch(b []byte, fn func(common.Hash, umaEv)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if num == 4 && typ == protowire.BytesType {
			ev, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return
			}
			b = b[m:]
			walkEvent(ev, fn)
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return
		}
		b = b[m:]
	}
}

type umaEv struct {
	eventType, upstream, marketID, category, betType, outcome uint64
	negRisk                                                   bool
}

func walkEvent(b []byte, fn func(common.Hash, umaEv)) {
	var h common.Hash
	var e umaEv
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		switch {
		case typ == protowire.VarintType && (num == 2 || num == 5 || num == 12 || num == 13 || num == 14 || num == 15):
			v, m := protowire.ConsumeVarint(b)
			b = b[m:]
			switch num {
			case 2:
				e.eventType = v
			case 5:
				e.marketID = v
			case 12:
				e.outcome = v
			case 13:
				e.category = v
			case 14:
				e.betType = v
			case 15:
				e.negRisk = v != 0
			}
		case num == 3 && typ == protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			h, b = common.BytesToHash(v), b[m:]
		case num == 9 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			e.upstream, b = v, b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return
			}
			b = b[m:]
		}
	}
	if h != (common.Hash{}) {
		fn(h, e)
	}
}

// ---- 启动 ----

func headFromRPC(url string) (uint64, uint64, error) {
	body := `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var r struct {
		Result struct {
			Number    string `json:"number"`
			Timestamp string `json:"timestamp"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return 0, 0, fmt.Errorf("%w: %s", err, raw)
	}
	n, ok1 := new(big.Int).SetString(strings.TrimPrefix(r.Result.Number, "0x"), 16)
	t, ok2 := new(big.Int).SetString(strings.TrimPrefix(r.Result.Timestamp, "0x"), 16)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("bad head response: %s", raw)
	}
	return n.Uint64(), t.Uint64(), nil
}

func loadKey(path string) *ecdsa.PrivateKey {
	if k, err := crypto.LoadECDSA(path); err == nil {
		return k
	}
	k, _ := crypto.GenerateKey()
	if err := crypto.SaveECDSA(path, k); err != nil {
		log.Printf("save nodekey: %v", err)
	}
	return k
}

func main() {
	umaURL := flag.String("uma", "ws://172.28.0.11:8011/uma/v1/ws", "rust-uma WSS")
	headRPC := flag.String("head-rpc", "https://polygon-bor-rpc.publicnode.com", "只在启动时取链头算 forkid")
	outPath := flag.String("out", "p2p_probe.ndjson", "ndjson 输出")
	duration := flag.Duration("duration", 30*time.Minute, "运行时长")
	observer := flag.String("observer", "", "观测点名字（多地三角定位时区分来源，如 fra/hkg/tyo）")
	pushURL := flag.String("push", "", "把事件行推给汇总端的 URL（含 token）")
	feedListen := flag.String("feed-listen", "", "本机推送 pending 交易的地址（给 mempool-uma），如 127.0.0.1:8014")
	maxPeers := flag.Int("max-peers", 50, "最大 peer 数")
	listen := flag.String("listen", ":30303", "P2P 监听地址")
	extIP := flag.String("ext-ip", "", "对外公布的公网 IP")
	keyPath := flag.String("nodekey", "nodekey", "节点私钥文件（保持身份稳定）")
	verbosity := flag.Int("verbosity", 0, "geth 日志级别：0=只看 warn，1=info，2=debug")
	peersPath := flag.String("peers-out", "peers.json", "每个 peer 贡献统计（每分钟覆盖写）")
	denyPath := flag.String("deny", "", "剔除名单：每行一个节点 ID（完整或前 16 位 hex）")
	staticPath := flag.String("static", "", "额外常驻节点：每行一个 enode URL")
	dialRatio := flag.Int("dial-ratio", 1, "出站拨号占 peer 槽位的比例倒数（1=全部主动拨出；入站被安全组挡着）")
	seatsPath := flag.String("seats", "seats.json", "席位状态文件（核心/试用/冷却，重启不丢）")
	seedPath := flag.String("seats-seed", "", "首次启动的核心种子：每行一个 enode URL（seats.json 不存在时才用）")
	trialSeats := flag.Int("trial-seats", 5, "试用席位数")
	coreMax := flag.Int("core-max", 30, "核心上限")
	evalEvery := flag.Duration("seat-eval", time.Hour, "席位评估周期")
	observeFor := flag.Duration("seat-observe", 2*time.Hour, "试用观察期")
	exploreHour := flag.Int("explore-hour", 20, "保底探索开始的 UTC 小时（-1 关闭）")
	geoCache := flag.String("geo-cache", "/var/lib/p2p-bayes/enrich_cache.json", "IP 归属缓存")
	flag.Parse()
	level := map[int]slog.Level{0: slog.LevelWarn, 1: slog.LevelInfo, 2: slog.LevelDebug, 3: glog.LevelTrace}[*verbosity]
	glog.SetDefault(glog.NewLogger(glog.NewTerminalHandlerWithLevel(os.Stderr, level, false)))
	// geth 的 SetDefault 会把标准库 log 也接进 slog（Info 级别，verbosity=0 时被吞），这里接回 stderr。
	log.SetOutput(os.Stderr)

	genesis := core.DefaultBorMainnetGenesisBlock().ToBlock()
	if genesis.Hash() != params.BorMainnetGenesisHash {
		log.Fatalf("genesis hash mismatch: %s", genesis.Hash())
	}
	head, headTime, err := headFromRPC(*headRPC)
	if err != nil {
		log.Fatalf("head rpc: %v", err)
	}
	cfg := params.BorMainnetChainConfig
	fid := forkid.NewID(cfg, genesis, head, headTime)
	staticFlt := forkid.NewStaticFilter(cfg, genesis)
	log.Printf("head=%d forkid=%x next=%d", head, fid.Hash, fid.Next)

	out, err := os.OpenFile(*outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	p := &probe{
		txs: map[common.Hash]*txInfo{}, blockTx: map[common.Hash]int64{}, blocks: map[common.Hash]bool{}, bodyReq: map[uint64]blockAnn{},
		uma: map[common.Hash]*umaInfo{}, out: out, signer: types.LatestSignerForChainID(cfg.ChainID),
		genesis: genesis.Hash(), td: genesis.Difficulty(), forkID: fid, staticFlt: staticFlt,
		peersByVer: map[uint]int{}, handshakeFail: map[string]int{}, msgCodes: map[string]int{}, decodeErrs: map[string]int{},
		peers: map[string]*peerStat{}, blockSeen: map[common.Hash]int64{}, deny: map[string]bool{},
		blockPeer: map[common.Hash]string{}, hdrReq: map[uint64]common.Hash{}, rcptReq: map[uint64]common.Hash{}, blockEmitted: map[common.Hash]bool{},
		borCfg: cfg.Bor, feedSubs: map[chan []byte]struct{}{},
	}
	readLines := func(path string) []string {
		if path == "" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("read %s: %v", path, err)
		}
		var out []string
		for _, l := range strings.Split(string(raw), "\n") {
			if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
				out = append(out, strings.Fields(l)[0])
			}
		}
		return out
	}
	p.seats = newSeatManager(seatConfig{
		Promote: 0.01, Demote: 0.007, DemoteRounds: 2, CoreMax: *coreMax, TrialSeats: *trialSeats,
		ObserveFor: *observeFor, ObserveTx: 200, EvalEvery: *evalEvery, SilentAfter: 10 * time.Minute,
		LostTolerance: 24 * time.Hour, HalfLife: 12 * time.Hour, TieUs: 1000, USFloor: 2,
		ExploreHour: *exploreHour, ExploreFor: time.Hour, ExploreExtra: 20, GeoCache: *geoCache,
	}, *seatsPath)
	p.seats.lastEval = time.Now()
	fresh := len(p.seats.seats) == 0
	if fresh {
		// 首次启动：种子 = 核心；剔除名单 = 冷却 72h
		for _, u := range readLines(*seedPath) {
			if n, err := enode.Parse(enode.ValidSchemes, u); err == nil {
				s := p.seats.get(n.ID().String()[:16])
				s.Enode, s.State, s.CoreSince = u, stCore, nowUs()
			}
		}
		for _, id := range readLines(*denyPath) {
			id = strings.TrimPrefix(strings.ToLower(id), "0x")
			if len(id) >= 16 {
				s := p.seats.get(id[:16])
				s.State, s.CooldownUntil, s.CooldownCount, s.LastReason = stCooldown, nowUs()+(72*time.Hour).Microseconds(), 1, "种子：历史静默节点"
			}
		}
	}

	var boot []*enode.Node
	for _, u := range params.BorMainnetBootnodes {
		if n, err := enode.Parse(enode.ValidSchemes, u); err == nil {
			boot = append(boot, n)
		}
	}
	var static []*enode.Node
	for _, u := range append(slices.Clone(polygonStatic), readLines(*staticPath)...) {
		if n, err := enode.Parse(enode.ValidSchemes, u); err == nil {
			static = append(static, n)
		}
	}
	srv := &p2p.Server{}
	flt := func(n *enode.Node) bool {
		var e ethEntry
		if !p.dialAllowed(n.ID().String()) {
			return false
		}
		if err := n.Load(&e); err != nil {
			return true // 没有 eth 字段的交给握手判断
		}
		return staticFlt(e.ForkID) == nil
	}
	iter := enode.NewFairMix(time.Second)
	iter.AddSource(&lazyIter{srv: srv, flt: flt})
	if dnsIter, err := dnsdisc.NewClient(dnsdisc.Config{}).NewIterator(polygonDNS); err == nil {
		iter.AddSource(enode.Filter(dnsIter, flt))
	} else {
		log.Printf("dns discovery: %v", err)
	}
	var protos []p2p.Protocol
	for _, v := range []uint{eth.ETH69, eth.ETH68} {
		length := uint64(17)
		if v == eth.ETH69 {
			length = 18
		}
		protos = append(protos, p2p.Protocol{
			Name: eth.ProtocolName, Version: v, Length: length,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				return p.run(peer, rw, v)
			},
			DialCandidates: iter,
		})
	}
	var natIf nat.Interface
	if *extIP != "" {
		natIf = nat.ExtIP(net.ParseIP(*extIP))
	}
	srv.Config = p2p.Config{
		PrivateKey: loadKey(*keyPath), MaxPeers: *maxPeers, MaxPendingPeers: 100, DialRatio: *dialRatio,
		Name: "bor/v2.10.1-stable/linux-amd64/go1.26.5", ListenAddr: *listen,
		DiscoveryV4: true, BootstrapNodes: append(boot, static...),
		Protocols: protos, NAT: natIf,
	}
	if err := srv.Start(); err != nil {
		log.Fatalf("p2p start: %v", err)
	}
	log.Printf("p2p started: %s", srv.Self().URLv4())
	p.mu.Lock()
	p.seats.srv = srv
	nCore := 0
	for _, s := range p.seats.seats {
		if s.State == stCore || s.State == stLost {
			if n := p.node(s); n != nil {
				srv.AddPeer(n)
				srv.AddTrustedPeer(n)
				nCore++
			}
		}
	}
	p.mu.Unlock()
	log.Printf("seats: %d core/lost restored, trial seats %d, core max %d", nCore, *trialSeats, *coreMax)

	p.observer = *observer
	if *feedListen != "" {
		go p.runFeed(*feedListen)
	}
	if *pushURL != "" {
		p.push = make(chan []byte, 100000)
		go p.runPush(*pushURL)
	}
	go p.runRTT()
	if *umaURL != "" {
		go p.runUma(*umaURL)
	}

	started := time.Now()
	day := ""
	p.rotateIfNeeded(*outPath, &day)
	deadline := time.After(*duration)
	tick := time.NewTicker(time.Minute)
	for {
		select {
		case <-tick.C:
			p.prune()
			p.seatTick()
			p.runOps()
			p.rotateIfNeeded(*outPath, &day)
			p.dumpPeers(*peersPath)
			s := p.summary(srv, time.Minute)
			p.mu.Lock()
			s["seats"] = p.seatSnapshot()
			p.mu.Unlock()
			p.emit(s)
			b, _ := json.Marshal(s)
			log.Printf("summary %s", b)
		case <-deadline:
			p.dumpPeers(*peersPath)
			s := p.summary(srv, time.Since(started))
			s["final"] = true
			p.emit(s)
			b, _ := json.Marshal(s)
			log.Printf("final %s", b)
			srv.Stop()
			return
		}
	}
}
