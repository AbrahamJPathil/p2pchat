// Package tui renders the Bubble Tea terminal interface.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"p2pchat/internal/peer"
	"p2pchat/internal/protocol"
)

const maxMessages = 200

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	dimStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("238")).
			Padding(0, 1)
	activeStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("212")).
			Padding(0, 1)
	statusStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("236")).
			Foreground(lipgloss.Color("252")).
			Padding(0, 1)
	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
)

type eventMsg peer.Event

// Model is the Bubble Tea application state.
type Model struct {
	p      *peer.Peer
	events <-chan peer.Event

	peers    []protocol.PeerInfo
	selected int
	focus    int // 0 = peer list, 1 = input

	messages []string
	input    textinput.Model
	bar      progress.Model

	status   string
	conn     peer.ConnType
	rtt      time.Duration
	sent     int64
	recv     int64
	file     string
	fileSize int64
	filePos  int64
	fileDone bool
	fileOK   bool

	width  int
	height int
}

// New builds the UI model for a running peer.
func New(p *peer.Peer) Model {
	in := textinput.New()
	in.Prompt = "> "
	in.Placeholder = "message, or /send <path>"
	in.CharLimit = 2000

	m := Model{
		p:      p,
		events: p.Events(),
		input:  in,
		bar:    progress.New(progress.WithDefaultGradient()),
		width:  100,
		height: 30,
		status: "starting",
	}
	return m
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, waitForEvent(m.events))
}

// Update implements tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.input.Width = max(10, m.width-12)
		leftW := min(34, max(24, m.width/4))
		m.bar.Width = max(10, m.width-leftW-8)
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "tab":
			if m.focus == 0 {
				m.focus = 1
				return m, m.input.Focus()
			}
			m.focus = 0
			m.input.Blur()
			return m, nil
		case "up", "k":
			if m.focus == 0 {
				m.moveSelection(-1)
				return m, nil
			}
		case "down", "j":
			if m.focus == 0 {
				m.moveSelection(1)
				return m, nil
			}
		case "enter":
			m.submit()
			return m, nil
		}

	case eventMsg:
		m.applyEvent(peer.Event(msg))
		return m, waitForEvent(m.events)
	}

	if m.focus == 1 {
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *Model) moveSelection(delta int) {
	if len(m.peers) == 0 {
		return
	}
	m.selected = (m.selected + delta + len(m.peers)) % len(m.peers)
}

func (m *Model) submit() {
	if m.focus == 0 {
		if len(m.peers) == 0 {
			m.status = "no peers online"
			return
		}
		name := m.peers[m.selected].Name
		m.status = "connecting to " + name
		m.p.Connect(name)
		return
	}

	text := strings.TrimSpace(m.input.Value())
	if text == "" {
		return
	}
	m.input.SetValue("")

	if strings.HasPrefix(text, "/send ") {
		path := strings.TrimSpace(strings.TrimPrefix(text, "/send "))
		if path == "" {
			m.status = "usage: /send <path>"
			return
		}
		m.messages = append(m.messages, "* sending "+path)
		m.p.SendFile(path)
		return
	}

	if err := m.p.SendChat(text); err != nil {
		m.status = "error: " + err.Error()
		return
	}
	m.messages = append(m.messages, "you: "+text)
}

func (m *Model) applyEvent(e peer.Event) {
	switch e.Kind {
	case peer.EventStatus:
		m.status = e.Text
	case peer.EventPeers:
		m.peers = e.Peers
		if m.selected >= len(m.peers) {
			m.selected = max(0, len(m.peers)-1)
		}
	case peer.EventChat:
		m.addMessage(fmt.Sprintf("%s: %s", e.Peer, e.Text))
	case peer.EventSession:
		if e.Text == "connected" {
			m.conn = e.Conn
			m.status = fmt.Sprintf("connected to %s (%s)", e.Peer, e.Conn)
			m.addMessage(fmt.Sprintf("* connected to %s via %s", e.Peer, e.Conn))
		} else {
			m.conn = ""
			m.rtt = 0
			m.status = "disconnected from " + e.Peer
			m.addMessage("* disconnected from " + e.Peer)
		}
	case peer.EventStats:
		m.conn = e.Conn
		m.rtt = e.RTT
		m.sent = e.Sent
		m.recv = e.Recv
	case peer.EventTransfer:
		m.file = e.File
		m.fileSize = e.Size
		m.filePos = e.Progress
		m.fileDone = e.Done
		m.fileOK = e.OK
		if e.Done {
			if e.OK {
				m.status = "transfer complete: " + e.File
			} else {
				m.status = "transfer failed integrity check: " + e.File
			}
		}
	case peer.EventError:
		if e.Err != nil {
			m.status = "error: " + e.Err.Error()
			m.addMessage("* error: " + e.Err.Error())
		}
	}
}

func (m *Model) addMessage(s string) {
	m.messages = append(m.messages, s)
	if len(m.messages) > maxMessages {
		m.messages = m.messages[len(m.messages)-maxMessages:]
	}
}

// View implements tea.Model.
func (m Model) View() string {
	w, h := m.width, m.height
	if w < 60 || h < 20 {
		return "terminal too small (need at least 60x20)"
	}

	bodyH := h - 3
	leftW := min(34, max(24, w/4))
	rightW := w - leftW
	chatH := max(6, bodyH*2/3)
	transferH := bodyH - chatH

	left := m.renderPeers(leftW, bodyH)
	chat := m.renderChat(rightW, chatH)
	transfer := m.renderTransfer(rightW, transferH)
	right := lipgloss.JoinVertical(lipgloss.Left, chat, transfer)
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, right)

	mode := "idle"
	if m.conn != "" {
		mode = string(m.conn)
	}
	rtt := "-"
	if m.rtt > 0 {
		rtt = m.rtt.Round(time.Millisecond).String()
	}
	status := fmt.Sprintf("%s | rtt %s | up %s | down %s | %s",
		mode, rtt, fmtBytes(m.sent), fmtBytes(m.recv), m.status)
	statusLine := statusStyle.Width(w - 2).Render(truncate(status, w-2))

	inputLine := "  " + m.input.View()
	help := helpStyle.Render("  tab switch | enter connect/send | /send file | ctrl+c quit")

	return lipgloss.JoinVertical(lipgloss.Left, body, statusLine, inputLine, help)
}

func (m Model) renderPeers(w, h int) string {
	style := panelStyle
	if m.focus == 0 {
		style = activeStyle
	}
	lines := []string{titleStyle.Render(fmt.Sprintf("PEERS (%d)", len(m.peers)))}
	if len(m.peers) == 0 {
		lines = append(lines, dimStyle.Render("waiting for peers..."))
	}
	room := h - 3
	start := 0
	if len(m.peers) > room && room > 0 {
		start = m.selected - room/2
		if start < 0 {
			start = 0
		}
		if start+room > len(m.peers) {
			start = len(m.peers) - room
		}
	}
	for i := start; i < len(m.peers) && i < start+room; i++ {
		p := m.peers[i]
		cursor := "  "
		if i == m.selected {
			cursor = "> "
		}
		lines = append(lines, truncate(cursor+p.Name+"  "+p.Addr, w-6))
	}
	return style.Width(w - 4).Height(h - 2).Render(strings.Join(lines, "\n"))
}

func (m Model) renderChat(w, h int) string {
	style := panelStyle
	if m.focus == 1 {
		style = activeStyle
	}
	room := max(1, h-4)
	lines := []string{titleStyle.Render("CHAT")}
	if len(m.messages) == 0 {
		lines = append(lines, dimStyle.Render("no messages yet"))
	} else {
		start := max(0, len(m.messages)-room)
		for _, msg := range m.messages[start:] {
			lines = append(lines, truncate(msg, w-6))
		}
	}
	return style.Width(w - 4).Height(h - 2).Render(strings.Join(lines, "\n"))
}

func (m Model) renderTransfer(w, h int) string {
	lines := []string{titleStyle.Render("TRANSFER")}
	if m.file == "" {
		lines = append(lines, dimStyle.Render("no transfer"))
	} else {
		pct := 0.0
		if m.fileSize > 0 {
			pct = float64(m.filePos) / float64(m.fileSize)
		}
		lines = append(lines, truncate(fmt.Sprintf("%s  %s / %s", m.file, fmtBytes(m.filePos), fmtBytes(m.fileSize)), w-6))
		lines = append(lines, m.bar.ViewAs(pct))
		switch {
		case m.fileDone && m.fileOK:
			lines = append(lines, "DONE - sha256 verified")
		case m.fileDone && !m.fileOK:
			lines = append(lines, "FAILED - sha256 mismatch")
		default:
			lines = append(lines, fmt.Sprintf("%.0f%%", pct*100))
		}
	}
	return panelStyle.Width(w - 4).Height(h - 2).Render(strings.Join(lines, "\n"))
}

func waitForEvent(ch <-chan peer.Event) tea.Cmd {
	return func() tea.Msg {
		return eventMsg(<-ch)
	}
}

func truncate(s string, w int) string {
	if w <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= w {
		return s
	}
	if w <= 3 {
		return string(r[:w])
	}
	return string(r[:w-3]) + "..."
}

func fmtBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
