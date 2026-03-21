// Vibecoded TUI, not gonna lie for you!

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/chroma/quick"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/rikatz/notyetanotherenvoybackend/client"
	"github.com/rikatz/notyetanotherenvoybackend/client/debugstore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Item structure for the list
type item struct {
	title, desc, body string
}

type Msg struct {
	Operation string
	Value     string
}

type RandomStuff struct {
	ID      int
	Message string
}

func (i item) Title() string       { return i.title }
func (i item) Description() string { return i.desc }
func (i item) FilterValue() string { return i.title }

type model struct {
	sub      chan Msg
	list     list.Model
	viewport viewport.Model
	ready    bool
}

func waitForActivity(sub chan Msg) tea.Cmd {
	return func() tea.Msg {
		return Msg(<-sub)
	}
}

func (m model) Init() tea.Cmd {
	return waitForActivity(m.sub)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		primaryColor := lipgloss.AdaptiveColor{Light: "#7D45E1", Dark: "#874BFD"}
		h, v := lipgloss.NewStyle().Foreground(primaryColor).Margin(1, 2).GetFrameSize()
		m.list.SetSize(msg.Width/3-h, msg.Height-v)
		m.viewport.Width = msg.Width*2/3 - h
		m.viewport.Height = msg.Height - v
		m.ready = true

	case Msg:
		// Add new JSON to the list

		newItem := item{
			title: msg.Operation,
			desc:  time.Now().Format("15:04:05"),
			body:  msg.Value,
		}
		m.list.InsertItem(len(m.list.Items()), newItem)

		// If it's the first message, display it immediately
		if len(m.list.Items()) == 1 {
			m.viewport.SetContent(colorizeJSON(newItem.body))
		}
		return m, waitForActivity(m.sub)

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	}

	// Update list and check if selection changed
	newList, listCmd := m.list.Update(msg)
	m.list = newList
	cmds = append(cmds, listCmd)

	// Update viewport content based on selection
	if i, ok := m.list.SelectedItem().(item); ok {
		m.viewport.SetContent(colorizeJSON(i.body))
	}

	var viewCmd tea.Cmd
	m.viewport, viewCmd = m.viewport.Update(msg)
	cmds = append(cmds, viewCmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	if !m.ready {
		return "Initializing..."
	}

	return lipgloss.JoinHorizontal(
		lipgloss.Top,
		lipgloss.NewStyle().Margin(1, 2).Render(m.list.View()),
		lipgloss.NewStyle().Margin(1, 2).Render(m.viewport.View()),
	)
}

func colorizeJSON(input string) string {
	var out bytes.Buffer
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(input), "", "  "); err != nil {
		return input
	}
	_ = quick.Highlight(&out, pretty.String(), "json", "terminal256", theme)
	return out.String()
}

var (
	agentgatewayEndpoint string
	tokenFile            string
	namespace            string
	gatewayname          string
	debug                bool
	theme                string
)

func main() {
	flag.StringVar(&agentgatewayEndpoint, "endpoint", "127.0.0.1:9978", "endpoint controller to connect to")
	flag.StringVar(&tokenFile, "token-file", "", "file containing the token")
	flag.StringVar(&namespace, "namespace", "agentgateway-system", "namespace where the gateway is running")
	flag.StringVar(&gatewayname, "gateway", "agentgateway-ricardo1", "gateway name")
	flag.BoolVar(&debug, "debug", false, "put in debug mode")
	flag.StringVar(&theme, "theme", "dracula", "Theme to use")

	flag.Parse()

	opts := &slog.HandlerOptions{
		Level: slog.LevelError,
	}

	handler := slog.NewTextHandler(&os.File{}, opts)
	slog.SetDefault(slog.New(handler))

	tokenBytes, err := os.ReadFile(tokenFile)
	if err != nil {
		panic(err)
	}

	addressStore := debugstore.NewAddressStore()
	resourceStore := debugstore.NewResourceStore()

	resourceHandlerStore := client.ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address":             addressStore,
		"type.googleapis.com/agentgateway.dev.resource.Resource": resourceStore,
	}

	token := strings.TrimSpace(string(tokenBytes))

	client, err := client.NewADSClient(client.Config{
		ServerAddress:        agentgatewayEndpoint,
		Namespace:            namespace,
		GatewayName:          gatewayname,
		Token:                token,
		ResourceTypeHandlers: resourceHandlerStore,
	})
	if err != nil {
		panic(err)
	}

	addressNotify, resourceNotify := addressStore.Notify(), resourceStore.Notify()

	go func() {
		if err := client.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	outputCh := make(chan Msg)
	go func() {
		for {
			select {
			case address := <-addressNotify:
				if address.Operation == "REMOVE" {
					outputCh <- removeJSON(address.ResourcesRemoved, "Address")
					continue
				}
				outputCh <- Msg{
					Operation: "UPDATE - Address",
					Value:     joinMessages(address.Resources),
				}

			case resource := <-resourceNotify:
				if resource.Operation == "REMOVE" {
					outputCh <- removeJSON(resource.ResourcesRemoved, "Resource")
					continue
				}
				outputCh <- Msg{
					Operation: "UPDATE - Resource",
					Value:     joinMessages(resource.Resources),
				}

			}
		}
	}()

	l := list.New([]list.Item{}, list.NewDefaultDelegate(), 0, 0)
	l.Title = "ADS Messages"

	p := tea.NewProgram(model{sub: outputCh, list: l, viewport: viewport.New(0, 0)}, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
}

func removeJSON(resources []string, resType string) Msg {
	msg := Msg{
		Operation: fmt.Sprintf("REMOVE - %s", resType),
		Value:     fmt.Sprintf("%+v", resources),
	}
	val, err := json.Marshal(resources)
	if err == nil {
		msg.Value = string(val)
	}
	return msg
}

func joinMessages(msgs []proto.Message) string {
	jsonMessages := make([]string, 0)
	for i := range msgs {
		resValue, err := protojson.Marshal(msgs[i])
		if err == nil {
			jsonMessages = append(jsonMessages, string(resValue))
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(jsonMessages, ","))
}
