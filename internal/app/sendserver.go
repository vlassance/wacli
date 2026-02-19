package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steipete/wacli/internal/store"
	"github.com/steipete/wacli/internal/wa"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// SendRequest is the JSON payload sent over the Unix socket.
type SendRequest struct {
	To       string `json:"to"`
	Message  string `json:"message,omitempty"`
	Type     string `json:"type,omitempty"`      // "text" (default) or "file"
	FilePath string `json:"file_path,omitempty"` // local path to file
	Filename string `json:"filename,omitempty"`  // display name override
	Caption  string `json:"caption,omitempty"`   // caption for media messages
	Mime     string `json:"mime,omitempty"`       // MIME type override
}

// SendResponse is the JSON response from the send server.
type SendResponse struct {
	Success bool              `json:"success"`
	ID      string            `json:"id,omitempty"`
	To      string            `json:"to,omitempty"`
	Error   string            `json:"error,omitempty"`
	File    map[string]string `json:"file,omitempty"`
}

// SendSocketPath returns the path to the send socket for a given store dir.
func SendSocketPath(storeDir string) string {
	return filepath.Join(storeDir, "send.sock")
}

// StartSendServer starts a Unix socket listener that accepts send requests
// and processes them through the existing WhatsApp connection.
func (a *App) StartSendServer(ctx context.Context) (func(), error) {
	sockPath := SendSocketPath(a.opts.StoreDir)

	// Remove stale socket from previous run.
	os.Remove(sockPath)

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", sockPath, err)
	}

	os.Chmod(sockPath, 0600)

	cleanup := func() {
		listener.Close()
		os.Remove(sockPath)
	}

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			go a.handleSendConn(ctx, conn)
		}
	}()

	fmt.Fprintf(os.Stderr, "Send server listening on %s\n", sockPath)
	return cleanup, nil
}

func (a *App) handleSendConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		writeSendResponse(conn, SendResponse{Error: "no input"})
		return
	}

	var req SendRequest
	if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
		writeSendResponse(conn, SendResponse{Error: "invalid json: " + err.Error()})
		return
	}

	if req.To == "" {
		writeSendResponse(conn, SendResponse{Error: "to is required"})
		return
	}

	toJID, err := wa.ParseUserOrJID(req.To)
	if err != nil {
		writeSendResponse(conn, SendResponse{Error: "invalid recipient: " + err.Error()})
		return
	}

	switch strings.ToLower(req.Type) {
	case "file":
		conn.SetDeadline(time.Now().Add(2 * time.Minute))
		a.handleSendFileSock(ctx, conn, req, toJID)
	default:
		if req.Message == "" {
			writeSendResponse(conn, SendResponse{Error: "to and message are required"})
			return
		}
		a.handleSendTextSock(ctx, conn, req, toJID)
	}
}

func (a *App) handleSendTextSock(ctx context.Context, conn net.Conn, req SendRequest, toJID types.JID) {
	msgID, err := a.wa.SendText(ctx, toJID, req.Message)
	if err != nil {
		writeSendResponse(conn, SendResponse{Error: "send failed: " + err.Error()})
		return
	}

	now := time.Now().UTC()
	chatName := a.wa.ResolveChatName(ctx, toJID, "")
	kind := chatKind(toJID)
	_ = a.db.UpsertChat(toJID.String(), kind, chatName, now)
	_ = a.db.UpsertMessage(store.UpsertMessageParams{
		ChatJID:    toJID.String(),
		ChatName:   chatName,
		MsgID:      string(msgID),
		SenderJID:  "",
		SenderName: "me",
		Timestamp:  now,
		FromMe:     true,
		Text:       req.Message,
	})

	writeSendResponse(conn, SendResponse{
		Success: true,
		ID:      string(msgID),
		To:      toJID.String(),
	})
}

func (a *App) handleSendFileSock(ctx context.Context, conn net.Conn, req SendRequest, toJID types.JID) {
	if req.FilePath == "" {
		writeSendResponse(conn, SendResponse{Error: "file_path is required for file sends"})
		return
	}

	data, err := os.ReadFile(req.FilePath)
	if err != nil {
		writeSendResponse(conn, SendResponse{Error: "read file: " + err.Error()})
		return
	}

	name := strings.TrimSpace(req.Filename)
	if name == "" {
		name = filepath.Base(req.FilePath)
	}

	mimeType := strings.TrimSpace(req.Mime)
	if mimeType == "" {
		mimeType = mime.TypeByExtension(strings.ToLower(filepath.Ext(req.FilePath)))
	}
	if mimeType == "" {
		sniff := data
		if len(sniff) > 512 {
			sniff = sniff[:512]
		}
		mimeType = http.DetectContentType(sniff)
	}

	mediaType := "document"
	uploadType, _ := wa.MediaTypeFromString("document")
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		mediaType = "image"
		uploadType, _ = wa.MediaTypeFromString("image")
	case strings.HasPrefix(mimeType, "video/"):
		mediaType = "video"
		uploadType, _ = wa.MediaTypeFromString("video")
	case strings.HasPrefix(mimeType, "audio/"):
		mediaType = "audio"
		uploadType, _ = wa.MediaTypeFromString("audio")
	}

	up, err := a.wa.Upload(ctx, data, uploadType)
	if err != nil {
		writeSendResponse(conn, SendResponse{Error: "upload failed: " + err.Error()})
		return
	}

	caption := req.Caption
	msg := &waProto.Message{}

	switch mediaType {
	case "image":
		msg.ImageMessage = &waProto.ImageMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mimeType),
			Caption:       proto.String(caption),
		}
	case "video":
		msg.VideoMessage = &waProto.VideoMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mimeType),
			Caption:       proto.String(caption),
		}
	case "audio":
		msg.AudioMessage = &waProto.AudioMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mimeType),
			PTT:           proto.Bool(false),
		}
	default:
		msg.DocumentMessage = &waProto.DocumentMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mimeType),
			FileName:      proto.String(name),
			Caption:       proto.String(caption),
			Title:         proto.String(name),
		}
	}

	msgID, err := a.wa.SendProtoMessage(ctx, toJID, msg)
	if err != nil {
		writeSendResponse(conn, SendResponse{Error: "send failed: " + err.Error()})
		return
	}

	now := time.Now().UTC()
	chatName := a.wa.ResolveChatName(ctx, toJID, "")
	kind := chatKind(toJID)
	_ = a.db.UpsertChat(toJID.String(), kind, chatName, now)
	_ = a.db.UpsertMessage(store.UpsertMessageParams{
		ChatJID:       toJID.String(),
		ChatName:      chatName,
		MsgID:         string(msgID),
		SenderJID:     "",
		SenderName:    "me",
		Timestamp:     now,
		FromMe:        true,
		Text:          caption,
		MediaType:     mediaType,
		MediaCaption:  caption,
		Filename:      name,
		MimeType:      mimeType,
		DirectPath:    up.DirectPath,
		MediaKey:      up.MediaKey,
		FileSHA256:    up.FileSHA256,
		FileEncSHA256: up.FileEncSHA256,
		FileLength:    up.FileLength,
	})

	writeSendResponse(conn, SendResponse{
		Success: true,
		ID:      string(msgID),
		To:      toJID.String(),
		File: map[string]string{
			"name":      name,
			"mime_type": mimeType,
			"media":     mediaType,
		},
	})
}

func writeSendResponse(conn net.Conn, resp SendResponse) {
	data, _ := json.Marshal(resp)
	conn.Write(data)
	conn.Write([]byte("\n"))
}
