package contracttest

import (
	"testing"
	"time"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
)

// rebalanceLimit: el reparto converge en menos de 15 s en la configuración de referencia.
const rebalanceLimit = 15 * time.Second

type partitionKey struct {
	topic     string
	partition int32
}

// member es un consumidor que reconcilia como exige el contrato: envía lo que
// tiene, deja lo que no figura en assignment y toma lo que figura.
type member struct {
	e      *env
	group  string
	id     string
	topics []string
	epoch  int32
	owned  map[partitionKey]bool
	last   *pb.HeartbeatResponse
}

func newMember(e *env, group, id string, topics ...string) *member {
	return &member{e: e, group: group, id: id, topics: topics, owned: map[partitionKey]bool{}}
}

func (m *member) heartbeat(epoch int32, topics []string) *pb.HeartbeatResponse {
	m.e.t.Helper()
	var owned []*pb.TopicPartition
	for k := range m.owned {
		owned = append(owned, tp(k.topic, k.partition))
	}
	resp, err := m.e.c.Groups.Heartbeat(m.e.ctx(), &pb.HeartbeatRequest{GroupId: m.group, MemberId: m.id,
		MemberEpoch: epoch, Topics: topics, OwnedPartitions: owned})
	if err != nil {
		m.e.t.Fatalf("Heartbeat de %s: %v", m.id, err)
	}
	m.last = resp
	if resp.GetError() == errNone && epoch >= 0 {
		m.epoch = resp.GetMemberEpoch()
		m.owned = map[partitionKey]bool{}
		for _, p := range resp.GetAssignment().GetPartitions() {
			m.owned[partitionKey{p.GetTopic(), p.GetPartition()}] = true
		}
	}
	return resp
}

func (m *member) join() *pb.HeartbeatResponse {
	m.owned = map[partitionKey]bool{}
	return m.heartbeat(0, m.topics)
}

func (m *member) beat() *pb.HeartbeatResponse { return m.heartbeat(m.epoch, nil) }

func (m *member) leave() *pb.HeartbeatResponse { return m.heartbeat(-1, nil) }

func (m *member) commit(epoch int32, partition int32, next int64) pb.ErrorCode {
	m.e.t.Helper()
	resp, err := m.e.c.Groups.CommitOffsets(m.e.ctx(), &pb.CommitOffsetsRequest{GroupId: m.group,
		MemberId: m.id, MemberEpoch: epoch,
		Offsets: []*pb.OffsetCommit{{TopicPartition: tp(m.topics[0], partition), NextOffset: next}}})
	if err != nil {
		m.e.t.Fatalf("CommitOffsets: %v", err)
	}
	return resp.GetError()
}

// converge hace latir a los miembros hasta que ninguno tiene particiones
// pendientes y cada partición de topic tiene exactamente un dueño. En cada
// ronda comprueba que ninguna partición tenga dos dueños.
func converge(t *testing.T, topic string, partitions int32, members ...*member) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		owners := map[partitionKey]string{}
		stable := true
		for _, m := range members {
			if m.beat().GetError() != errNone {
				t.Fatalf("latido de %s: %v", m.id, m.last.GetError())
			}
			if len(m.last.GetPending().GetPartitions()) > 0 {
				stable = false
			}
			for k := range m.owned {
				if other, dup := owners[k]; dup {
					t.Fatalf("%v tiene dos dueños: %s y %s", k, other, m.id)
				}
				owners[k] = m.id
			}
		}
		if stable && int32(len(owners)) == partitions {
			return time.Since(start)
		}
		if time.Since(start) > rebalanceLimit {
			t.Fatalf("el reparto no convergió en %v: %v", rebalanceLimit, owners)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *env) committed(group, topic string, partitions ...int32) map[int32]int64 {
	e.t.Helper()
	var tps []*pb.TopicPartition
	for _, p := range partitions {
		tps = append(tps, tp(topic, p))
	}
	resp, err := e.c.Groups.FetchCommittedOffsets(e.ctx(), &pb.FetchCommittedOffsetsRequest{GroupId: group,
		TopicPartitions: tps})
	if err != nil || resp.GetError() != errNone {
		e.t.Fatalf("FetchCommittedOffsets: %v %v", err, resp.GetError())
	}
	out := map[int32]int64{}
	for _, o := range resp.GetOffsets() {
		out[o.GetTopicPartition().GetPartition()] = o.GetNextOffset()
	}
	return out
}

// Comprobación 5 (básico): asignación exclusiva, tiempo de reequilibrio,
// rechazo de commits de épocas antiguas y progreso que no retrocede.
func TestGroup_MembersJoin_ExclusiveAssignmentAndFencedCommits(t *testing.T) {
	e := connect(t)
	topic := e.topic(4, 3)
	group := name("g")
	c1 := newMember(e, group, "c1", topic)
	c2 := newMember(e, group, "c2", topic)

	first := c1.join()
	if first.GetError() != errNone {
		t.Fatalf("entrar: %v", first.GetError())
	}
	c2.join()
	converge(t, topic, 4, c1, c2)
	if len(c1.owned) != 2 || len(c2.owned) != 2 {
		t.Fatalf("range con 4 particiones y 2 miembros: %v y %v", c1.owned, c2.owned)
	}
	if c2.last.GetGroupEpoch() <= first.GetGroupEpoch() {
		t.Fatalf("la época del grupo no aumentó al entrar c2: %d → %d", first.GetGroupEpoch(),
			c2.last.GetGroupEpoch())
	}

	// Un tercer miembro: el reparto vuelve a converger dentro del límite.
	c3 := newMember(e, group, "c3", topic)
	c3.join()
	took := converge(t, topic, 4, c1, c2, c3)
	t.Logf("reequilibrio con un tercer miembro: %v", took)

	var mine int32 = -1
	for k := range c1.owned {
		mine = k.partition
	}
	if mine < 0 {
		t.Fatal("c1 se quedó sin particiones")
	}
	if code := c1.commit(c1.epoch, mine, 10); code != errNone {
		t.Fatalf("commit vigente: %v", code)
	}
	if code := c1.commit(c1.epoch-1, mine, 20); code != pb.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION {
		t.Fatalf("commit con época antigua: %v", code)
	}
	if code := c1.commit(c1.epoch, mine, 5); code != errNone {
		t.Fatalf("commit menor: %v", code)
	}
	if got := e.committed(group, topic, mine)[mine]; got != 10 {
		t.Fatalf("el progreso retrocedió: %d", got)
	}
	for p := range int32(4) {
		if !c1.owned[partitionKey{topic, p}] {
			if code := c1.commit(c1.epoch, p, 1); code != pb.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION {
				t.Fatalf("commit de %d, que no es de c1: %v", p, code)
			}
			break
		}
	}
}

// Comprobación 4 (básico): un consumidor reiniciado reanuda desde su progreso confirmado.
func TestGroup_RestartedMember_ResumesFromCommittedOffset(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	group := name("g")
	e.produce(topic, 0, "a", "b", "c", "d")

	c1 := newMember(e, group, "c1", topic)
	c1.join()
	converge(t, topic, 1, c1)
	if code := c1.commit(c1.epoch, 0, 3); code != errNone {
		t.Fatalf("commit: %v", code)
	}
	// Reinicio: el proceso pierde su estado y vuelve a entrar con su member_id.
	restarted := newMember(e, group, "c1", topic)
	restarted.join()
	converge(t, topic, 1, restarted)
	next := e.committed(group, topic, 0)[0]
	if next != 3 {
		t.Fatalf("offset confirmado tras reiniciar: %d", next)
	}
	if got := e.readAll(topic, 0, next); len(got) != 1 || string(got[0].Value) != "d" {
		t.Fatalf("reanudación desde %d: %+v", next, got)
	}
	// Una partición sin commit no aparece.
	if got := e.committed(name("otro"), topic, 0); len(got) != 0 {
		t.Fatalf("grupo sin commits: %v", got)
	}
}

// Comprobación 3 (básico), reprocesamiento: ResetGroupOffsets mueve la posición
// del grupo, también hacia atrás, solo con el grupo vacío.
func TestResetGroupOffsets_OnlyWhenEmpty_MovesPositionBack(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	group := name("g")
	c1 := newMember(e, group, "c1", topic)
	c1.join()
	converge(t, topic, 1, c1)
	c1.commit(c1.epoch, 0, 8)

	reset := &pb.ResetGroupOffsetsRequest{RequestId: name("req"), GroupId: group,
		Offsets: []*pb.OffsetCommit{{TopicPartition: tp(topic, 0), NextOffset: 2}}}
	resp, err := e.c.Admin.ResetGroupOffsets(e.ctx(), reset)
	if err != nil || resp.GetError() != pb.ErrorCode_ERROR_CODE_GROUP_NOT_EMPTY {
		t.Fatalf("reset con un miembro: %v %v", err, resp.GetError())
	}
	c1.leave()
	for range 2 { // repetir la misma petición deja el mismo estado
		resp, err = e.c.Admin.ResetGroupOffsets(e.ctx(), reset)
		if err != nil || resp.GetError() != errNone {
			t.Fatalf("reset con el grupo vacío: %v %v", err, resp.GetError())
		}
	}
	if got := e.committed(group, topic, 0)[0]; got != 2 {
		t.Fatalf("posición tras el reset: %d", got)
	}
}

// Épocas obsoletas, miembros desconocidos y salida idempotente.
func TestHeartbeat_UnknownOrStaleMembers_AreRejectedAndLeaveIsIdempotent(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	group := name("g")
	ghost := newMember(e, group, "nadie", topic)
	if code := ghost.heartbeat(3, nil).GetError(); code != pb.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER {
		t.Fatalf("latido de un miembro desconocido: %v", code)
	}
	c1 := newMember(e, group, "c1", topic)
	c1.join()
	converge(t, topic, 1, c1)
	if code := c1.heartbeat(c1.epoch+7, nil).GetError(); code != pb.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION {
		t.Fatalf("latido con otra época: %v", code)
	}
	for i := range 2 {
		if code := c1.leave().GetError(); code != errNone {
			t.Fatalf("salida %d: %v", i+1, code)
		}
	}
	bad := newMember(e, "a/b", "c1", topic)
	if code := bad.join().GetError(); code != errInvalidRequest {
		t.Fatalf("group_id con «/»: %v", code)
	}
}
