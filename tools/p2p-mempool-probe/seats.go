package main

// 节点席位管理：核心（≥1% 推送贡献，常驻）+ 试用（固定席位，末位淘汰）+ 冷却。
// 规则见知识库《法兰克福P2P节点生命周期-2026-09-29》，参数见 seatConfig。
//
// 三项贡献（任一达标即高价值）：
//   A propose 首达份额   B 独有领先份额（首达时比第二名早的毫秒数）   C 新块首达份额
// 份额 = 该节点累计贡献 ÷ 它在线期间全网的同类总量；分子分母每个评估周期按同一因子衰减（半衰期 12h）。
// 1ms 内同时送到算并列，功劳平分。

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const (
	stCandidate = "candidate"
	stTrial     = "trial"
	stCore      = "core"
	stLost      = "lost"
	stCooldown  = "cooldown"
)

type seatConfig struct {
	Promote       float64       // 晋升阈值（份额）
	Demote        float64       // 降级阈值
	DemoteRounds  int           // 连续几个周期低于降级阈值才降级
	CoreMax       int           // 核心上限
	TrialSeats    int           // 试用席位
	ObserveFor    time.Duration // 观察期：在线时长
	ObserveTx     float64       // 观察期：在线期间全网 propose 数
	EvalEvery     time.Duration // 评估周期
	SilentAfter   time.Duration // 连上多久零转发算静默
	LostTolerance time.Duration // 核心失联多久让位
	HalfLife      time.Duration // 份额半衰期
	TieUs         int64         // 并列窗口
	USFloor       int           // 核心里至少保留的美国节点数
	ExploreHour   int           // 保底探索开始的 UTC 小时（<0 关闭）
	ExploreFor    time.Duration
	ExploreExtra  int
	GeoCache      string // IP 归属缓存（bayes_monitor 维护的 enrich_cache.json）
}

type seat struct {
	ID            string  `json:"id"`
	Enode         string  `json:"enode"`
	Addr          string  `json:"addr"`
	Name          string  `json:"name"`
	State         string  `json:"state"`
	NA            float64 `json:"na"`
	NB            float64 `json:"nb"`
	NC            float64 `json:"nc"`
	DA            float64 `json:"da"`
	DB            float64 `json:"db"`
	DC            float64 `json:"dc"`
	Below         int     `json:"below"`
	CooldownUntil int64   `json:"cooldown_until_us"`
	CooldownCount int     `json:"cooldown_count"`
	CoreSince     int64   `json:"core_since_us"`
	LostSince     int64   `json:"lost_since_us"`
	ObsDone       bool    `json:"obs_done"`
	LastReason    string  `json:"last_reason"`

	online       bool
	explore      bool
	peer         *p2p.Peer
	g0A          float64 // 上次结算在线分母时的全网累计量
	g0B          float64
	g0C          float64
	sessionStart int64
	sessionTx    float64 // 本次连接时全网 propose 原始累计量
	sessionAnn   int64   // 本次连接时它的 TxAnn
	winA         float64 // 保底探索窗口内的贡献
	winC         float64
}

func (s *seat) shares() (a, b, c float64) {
	if s.DA > 0 {
		a = s.NA / s.DA
	}
	if s.DB > 0 {
		b = s.NB / s.DB
	}
	if s.DC > 0 {
		c = s.NC / s.DC
	}
	return
}

func (s *seat) score() float64 {
	a, b, c := s.shares()
	return math.Max(a, math.Max(b, c))
}

type seatManager struct {
	cfg   seatConfig
	path  string
	seats map[string]*seat
	srv   *p2p.Server
	// 全网原始累计量（不衰减）
	gA, gB, gC float64
	// 块并列：hash -> 已分到功劳的 peer
	blockTie map[common.Hash][]string
	// 保底探索
	exploring            bool
	winStart             int64
	winGA, winGC         float64
	winExploreA, winExpC float64
	geo                  map[string]string // ip -> 国家代码
	geoLoaded            time.Time
	lastEval             time.Time
	// 对 p2p.Server 的操作不能在持 probe.mu 时做：RemovePeer 会等对方断开，
	// 断开又要进 run() 的 defer 拿锁，会死锁。先排队，解锁后再执行。
	ops []func()
}

// runOps 在不持 probe.mu 时调用。
func (p *probe) runOps() {
	p.mu.Lock()
	ops := p.seats.ops
	p.seats.ops = nil
	p.mu.Unlock()
	for _, op := range ops {
		op()
	}
}

func newSeatManager(cfg seatConfig, path string) *seatManager {
	m := &seatManager{cfg: cfg, path: path, seats: map[string]*seat{}, blockTie: map[common.Hash][]string{}, geo: map[string]string{}}
	if b, err := os.ReadFile(path); err == nil {
		var rows []*seat
		if json.Unmarshal(b, &rows) == nil {
			for _, s := range rows {
				if s.State == stTrial {
					s.State = stCandidate // 重启后试用重新排队；核心、失联、冷却照旧
				}
				m.seats[s.ID] = s
			}
		}
	}
	return m
}

func (m *seatManager) get(id string) *seat {
	s := m.seats[id]
	if s == nil {
		s = &seat{ID: id, State: stCandidate}
		m.seats[id] = s
	}
	return s
}

func (m *seatManager) flush(s *seat) {
	if !s.online {
		return
	}
	s.DA += m.gA - s.g0A
	s.DB += m.gB - s.g0B
	s.DC += m.gC - s.g0C
	s.g0A, s.g0B, s.g0C = m.gA, m.gB, m.gC
}

func (m *seatManager) trialOnline() int {
	n := 0
	for _, s := range m.seats {
		if s.online && s.State == stTrial {
			n++
		}
	}
	return n
}

func (m *seatManager) trialCap() int {
	if m.exploring {
		return m.cfg.TrialSeats + m.cfg.ExploreExtra
	}
	return m.cfg.TrialSeats
}

// ---- 贡献记账（需持 probe.mu）

func (p *probe) seatCreditTx(seen []peerSeen, second int64) {
	m := p.seats
	if m == nil || len(seen) == 0 || seen[0].delay != 0 {
		return
	}
	var tie []string
	for _, s := range seen {
		if s.delay <= m.cfg.TieUs {
			tie = append(tie, s.peer)
		}
	}
	m.gA++
	share := 1 / float64(len(tie))
	for _, id := range tie {
		if s := m.seats[id]; s != nil && s.online {
			s.NA += share
			if s.explore || m.exploring {
				s.winA += share
			}
			if s.explore {
				m.winExploreA += share
			}
		}
	}
	if len(tie) == 1 && second > 0 {
		gain := float64(second) / 1000
		m.gB += gain
		if s := m.seats[tie[0]]; s != nil && s.online {
			s.NB += gain
		}
	}
}

func (p *probe) seatCreditBlock(peer string, hash common.Hash, first bool, delayUs int64) {
	m := p.seats
	if m == nil {
		return
	}
	if first {
		m.gC++
		m.blockTie[hash] = []string{peer}
		if s := m.seats[peer]; s != nil && s.online {
			s.NC++
			if s.explore || m.exploring {
				s.winC++
			}
			if s.explore {
				m.winExpC++
			}
		}
		return
	}
	lst, ok := m.blockTie[hash]
	if !ok || delayUs > m.cfg.TieUs || len(lst) == 0 {
		return
	}
	for _, id := range lst {
		if id == peer {
			return
		}
	}
	n := float64(len(lst))
	for _, id := range lst { // 每个老的并列者让出 1/n - 1/(n+1)
		if s := m.seats[id]; s != nil {
			d := 1/n - 1/(n+1)
			s.NC -= d
			if s.explore {
				m.winExpC -= d
			}
		}
	}
	if s := m.seats[peer]; s != nil && s.online {
		s.NC += 1 / (n + 1)
		if s.explore {
			m.winExpC += 1 / (n + 1)
		}
	}
	m.blockTie[hash] = append(lst, peer)
}

// ---- 准入（run() 握手前调用，需持 probe.mu）。返回 nil 表示放行。

func (p *probe) seatAdmit(id string, peer *p2p.Peer, ps *peerStat) error {
	m := p.seats
	s := m.get(id)
	now := nowUs()
	if s.State == stCooldown && now < s.CooldownUntil {
		return fmt.Errorf("cooldown")
	}
	switch s.State {
	case stCore, stLost:
		s.State, s.LostSince = stCore, 0
	default:
		if m.trialOnline() >= m.trialCap() {
			return p2p.DiscTooManyPeers
		}
		if s.State != stTrial {
			s.State, s.ObsDone = stTrial, false
		}
		s.explore = m.exploring && m.trialOnline() >= m.cfg.TrialSeats
	}
	s.online, s.peer = true, peer
	s.Enode, s.Addr, s.Name = peer.Node().URLv4(), peer.RemoteAddr().String(), peer.Fullname()
	s.g0A, s.g0B, s.g0C = m.gA, m.gB, m.gC
	s.sessionStart, s.sessionTx, s.sessionAnn = now, m.gA, ps.TxAnn
	return nil
}

func (p *probe) seatLeave(id string) {
	m := p.seats
	s := m.seats[id]
	if s == nil || !s.online {
		return
	}
	m.flush(s)
	s.online, s.peer, s.explore = false, nil, false
	switch s.State {
	case stCore:
		s.State, s.LostSince = stLost, nowUs()
	case stTrial:
		s.State = stCandidate
	}
}

// dialAllowed 给拨号候选过滤用（不持锁调用）。
func (p *probe) dialAllowed(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.seats
	if s := m.seats[id[:16]]; s != nil {
		if s.State == stCooldown && nowUs() < s.CooldownUntil {
			return false
		}
		if s.State == stCore || s.State == stLost {
			return true
		}
	}
	return m.trialOnline() < m.trialCap()
}

// ---- 状态变更与留痕（需持 probe.mu）

func (p *probe) decide(s *seat, to, reason string) {
	a, b, c := s.shares()
	from := s.State
	online := 0.0
	if s.online {
		online = float64(nowUs()-s.sessionStart) / 60e6
	}
	p.emitLocked(map[string]any{
		"kind": "peer_decision", "at_us": nowUs(), "id": s.ID, "addr": s.Addr, "name": s.Name, "from": from, "to": to, "reason": reason,
		"share_a": a, "share_b": b, "share_c": c, "online_min": online, "cooldown_count": s.CooldownCount,
	})
	log.Printf("seat %s %s -> %s (%s) A=%.2f%% B=%.2f%% C=%.2f%%", s.ID, from, to, reason, a*100, b*100, c*100)
	s.State, s.LastReason = to, reason
}

func (p *probe) node(s *seat) *enode.Node {
	if s.Enode == "" {
		return nil
	}
	n, err := enode.Parse(enode.ValidSchemes, s.Enode)
	if err != nil {
		return nil
	}
	return n
}

func (p *probe) toCore(s *seat, reason string) {
	p.decide(s, stCore, reason)
	s.CoreSince, s.Below, s.LostSince = nowUs(), 0, 0
	if n := p.node(s); n != nil && p.seats.srv != nil {
		srv := p.seats.srv
		p.seats.ops = append(p.seats.ops, func() { srv.AddPeer(n); srv.AddTrustedPeer(n) })
	}
}

func (p *probe) coreToTrial(s *seat, reason string) {
	p.decide(s, stTrial, reason)
	s.ObsDone, s.Below = true, 0
	if n := p.node(s); n != nil && p.seats.srv != nil {
		srv := p.seats.srv
		p.seats.ops = append(p.seats.ops, func() { srv.RemoveTrustedPeer(n) }) // 仍保持连接，按试用规则继续比
	}
}

func (p *probe) toCooldown(s *seat, reason string, silent bool) {
	s.CooldownCount++
	d := 24 * time.Hour
	switch {
	case s.CooldownCount >= 3:
		d = 7 * 24 * time.Hour
	case s.CooldownCount == 2 || silent:
		d = 72 * time.Hour
	}
	s.CooldownUntil = nowUs() + d.Microseconds()
	p.decide(s, stCooldown, fmt.Sprintf("%s，冷却 %s", reason, d))
	p.drop(s)
}

func (p *probe) drop(s *seat) {
	n, peer, srv := p.node(s), s.peer, p.seats.srv
	p.seats.ops = append(p.seats.ops, func() {
		if n != nil && srv != nil {
			srv.RemoveTrustedPeer(n)
			srv.RemovePeer(n) // 移出常驻并断开（会等到对方断开为止）
		}
		if peer != nil {
			peer.Disconnect(p2p.DiscUselessPeer)
		}
	})
}

func (m *seatManager) isUS(s *seat) bool {
	ip := s.Addr
	if i := strings.LastIndex(ip, ":"); i > 0 {
		ip = ip[:i]
	}
	return m.geo[ip] == "US"
}

func (m *seatManager) loadGeo() {
	if m.cfg.GeoCache == "" || time.Since(m.geoLoaded) < 10*time.Minute {
		return
	}
	m.geoLoaded = time.Now()
	b, err := os.ReadFile(m.cfg.GeoCache)
	if err != nil {
		return
	}
	var d struct {
		Geo map[string]struct {
			CC string `json:"cc"`
		} `json:"geo"`
	}
	if json.Unmarshal(b, &d) == nil {
		for ip, g := range d.Geo {
			m.geo[ip] = g.CC
		}
	}
}

// ---- 每分钟：静默检查、保底探索开关；到点做评估

func (p *probe) seatTick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.seats
	now := nowUs()
	// 静默：非核心连上 10 分钟零转发
	for id, s := range m.seats {
		if !s.online || s.State == stCore {
			continue
		}
		ps := p.peers[id]
		if ps != nil && now-s.sessionStart >= m.cfg.SilentAfter.Microseconds() && ps.TxAnn-s.sessionAnn == 0 {
			p.toCooldown(s, "静默：连上 10 分钟零转发", true)
		}
	}
	// 冷却期满回候选
	for _, s := range m.seats {
		if s.State == stCooldown && now >= s.CooldownUntil {
			p.decide(s, stCandidate, "冷却期满")
		}
	}
	p.exploreTick()
	if time.Since(m.lastEval) >= m.cfg.EvalEvery {
		m.lastEval = time.Now()
		p.seatEval()
	}
}

func (p *probe) seatEval() {
	m := p.seats
	m.loadGeo()
	now := nowUs()
	f := math.Pow(0.5, m.cfg.EvalEvery.Hours()/m.cfg.HalfLife.Hours())
	for _, s := range m.seats {
		m.flush(s)
		s.NA, s.NB, s.NC, s.DA, s.DB, s.DC = s.NA*f, s.NB*f, s.NC*f, s.DA*f, s.DB*f, s.DC*f
	}
	cores := func() []*seat {
		var out []*seat
		for _, s := range m.seats {
			if s.State == stCore {
				out = append(out, s)
			}
		}
		return out
	}
	usCore := 0
	for _, s := range cores() {
		if m.isUS(s) {
			usCore++
		}
	}
	// 1. 核心降级（连续 2 个周期三项都 <0.7%；美国保底）
	for _, s := range cores() {
		if !s.online || s.DA < 20 {
			continue
		}
		if s.score() < m.cfg.Demote {
			s.Below++
		} else {
			s.Below = 0
		}
		if s.Below >= m.cfg.DemoteRounds {
			if m.isUS(s) && usCore <= m.cfg.USFloor {
				continue
			}
			if m.isUS(s) {
				usCore--
			}
			p.coreToTrial(s, fmt.Sprintf("连续 %d 个周期三项都低于 %.1f%%", s.Below, m.cfg.Demote*100))
		}
		// 稳定 7 天清零冷却累计
		if s.CoreSince > 0 && now-s.CoreSince > (7*24*time.Hour).Microseconds() {
			s.CooldownCount = 0
		}
	}
	// 失联超时让位
	for _, s := range m.seats {
		if s.State == stLost && s.LostSince > 0 && now-s.LostSince > m.cfg.LostTolerance.Microseconds() {
			p.decide(s, stCandidate, "核心失联超过 24 小时")
			p.drop(s)
		}
	}
	// 2. 试用晋升
	obsDone := func(s *seat) bool {
		return s.ObsDone || (now-s.sessionStart >= m.cfg.ObserveFor.Microseconds() && m.gA-s.sessionTx >= m.cfg.ObserveTx)
	}
	for _, s := range m.seats {
		if s.State == stTrial && s.online && !s.explore && obsDone(s) {
			s.ObsDone = true
			if s.score() >= m.cfg.Promote {
				a, b, c := s.shares()
				p.toCore(s, fmt.Sprintf("观察期满达标 A=%.2f%% B=%.2f%% C=%.2f%%", a*100, b*100, c*100))
			}
		}
	}
	// 核心上限
	cs := cores()
	if len(cs) > m.cfg.CoreMax {
		sort.Slice(cs, func(i, j int) bool { return cs[i].score() < cs[j].score() })
		for _, s := range cs[:len(cs)-m.cfg.CoreMax] {
			p.coreToTrial(s, fmt.Sprintf("核心超过上限 %d，份额最低", m.cfg.CoreMax))
		}
	}
	// 3. 末位淘汰：观察期满、未晋升的试用里最低的 1 个
	var worst *seat
	for _, s := range m.seats {
		if s.State == stTrial && s.online && !s.explore && s.ObsDone {
			if worst == nil || s.score() < worst.score() {
				worst = s
			}
		}
	}
	if worst != nil {
		p.toCooldown(worst, fmt.Sprintf("末位淘汰（综合分 %.2f%%）", worst.score()*100), false)
	}
	m.saveLocked()
}

// ---- 保底探索：每天 1 小时多开席位，统计精选之外漏了多少

func (p *probe) exploreTick() {
	m := p.seats
	if m.cfg.ExploreHour < 0 {
		return
	}
	now := time.Now().UTC()
	in := now.Hour() == m.cfg.ExploreHour && now.Minute() < int(m.cfg.ExploreFor.Minutes())
	switch {
	case in && !m.exploring:
		m.exploring, m.winStart = true, nowUs()
		m.winGA, m.winGC, m.winExploreA, m.winExpC = m.gA, m.gC, 0, 0
		for _, s := range m.seats {
			s.winA, s.winC = 0, 0
		}
		p.emitLocked(map[string]any{"kind": "explore_start", "extra": m.cfg.ExploreExtra})
	case !in && m.exploring:
		m.exploring = false
		totA, totC := m.gA-m.winGA, m.gC-m.winGC
		missA, missC := 0.0, 0.0
		if totA > 0 {
			missA = m.winExploreA / totA
		}
		if totC > 0 {
			missC = m.winExpC / totC
		}
		kept, dropped := 0, 0
		for _, s := range m.seats {
			if !s.explore {
				continue
			}
			s.explore = false
			if (totA > 0 && s.winA/totA >= m.cfg.Promote) || (totC > 0 && s.winC/totC >= m.cfg.Promote) {
				kept++
				continue // 留作试用，不占淘汰名额（观察期照常计算）
			}
			dropped++
			p.decide(s, stCandidate, "保底探索结束，未达标，不进冷却")
			if peer := s.peer; peer != nil {
				m.ops = append(m.ops, func() { peer.Disconnect(p2p.DiscRequested) })
			}
		}
		rec := map[string]any{"kind": "explore_report", "window_min": float64(nowUs()-m.winStart) / 60e6,
			"propose_total": totA, "block_total": totC, "miss_ratio_propose": missA, "miss_ratio_block": missC,
			"kept": kept, "dropped": dropped, "alert": missA > 0.01 || missC > 0.01}
		p.emitLocked(rec)
		if missA > 0.01 || missC > 0.01 {
			log.Printf("ALERT explore: 精选之外漏看 propose %.2f%% / 块 %.2f%%，核心名单可能在老化", missA*100, missC*100)
		}
	}
}

// ---- 持久化

func (m *seatManager) saveLocked() {
	var rows []*seat
	for _, s := range m.seats {
		if s.State == stCandidate && s.NA+s.NC == 0 && s.CooldownCount == 0 {
			continue // 没价值信息的候选不落盘
		}
		rows = append(rows, s)
	}
	b, _ := json.MarshalIndent(rows, "", " ")
	tmp := m.path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, m.path)
	}
}

// seatSnapshot 给 peers.json / 网页用。
func (p *probe) seatSnapshot() map[string]any {
	m := p.seats
	counts := map[string]int{}
	for _, s := range m.seats {
		m.flush(s)
		k := s.State
		if s.online && s.State == stTrial && s.explore {
			k = "explore"
		}
		counts[k]++
	}
	return map[string]any{"counts": counts, "trial_online": m.trialOnline(), "trial_cap": m.trialCap(), "exploring": m.exploring}
}

// ---- 数据文件按天切分，旧文件压缩，保留 14 天

func (p *probe) rotateIfNeeded(outPath string, day *string) {
	today := time.Now().UTC().Format("20060102")
	if *day == "" {
		*day = today
		return
	}
	if today == *day {
		return
	}
	p.mu.Lock()
	p.out.Close()
	old := strings.TrimSuffix(outPath, ".ndjson") + "-" + *day + ".ndjson"
	os.Rename(outPath, old)
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("rotate: %v", err)
	}
	p.out = f
	p.mu.Unlock()
	*day = today
	go func() {
		exec.Command("gzip", "-f", old).Run()
		matches, _ := filepath.Glob(strings.TrimSuffix(outPath, ".ndjson") + "-*.ndjson.gz")
		for _, mth := range matches {
			if st, err := os.Stat(mth); err == nil && time.Since(st.ModTime()) > 14*24*time.Hour {
				os.Remove(mth)
			}
		}
	}()
}
