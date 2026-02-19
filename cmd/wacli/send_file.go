package main

import (
	"context"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steipete/wacli/internal/app"
	"github.com/steipete/wacli/internal/store"
	"github.com/steipete/wacli/internal/wa"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func sendFile(ctx context.Context, a interface {
	WA() app.WAClient
	DB() *store.DB
}, to types.JID, filePath, filename, caption, mimeOverride, replyTo, replyToParticipant string) (string, map[string]string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", nil, err
	}

	name := strings.TrimSpace(filename)
	if name == "" {
		name = filepath.Base(filePath)
	}
	mimeType := strings.TrimSpace(mimeOverride)
	if mimeType == "" {
		// Use filePath for MIME detection, not the display name override
		mimeType = mime.TypeByExtension(strings.ToLower(filepath.Ext(filePath)))
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

	up, err := a.WA().Upload(ctx, data, uploadType)
	if err != nil {
		return "", nil, err
	}

	// Build reply context if replying to a message.
	var ctxInfo *waProto.ContextInfo
	if strings.TrimSpace(replyTo) != "" {
		participant := strings.TrimSpace(replyToParticipant)
		if strings.EqualFold(participant, "self") {
			participant = a.WA().GetOwnJID().String()
		}
		ctxInfo = &waProto.ContextInfo{
			StanzaID:    proto.String(replyTo),
			Participant: proto.String(participant),
		}
	}

	now := time.Now().UTC()
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
			ContextInfo:   ctxInfo,
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
			ContextInfo:   ctxInfo,
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
			ContextInfo:   ctxInfo,
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
			ContextInfo:   ctxInfo,
		}
	}

	id, err := a.WA().SendProtoMessage(ctx, to, msg)
	if err != nil {
		return "", nil, err
	}

	chatName := a.WA().ResolveChatName(ctx, to, "")
	kind := chatKindFromJID(to)
	_ = a.DB().UpsertChat(to.String(), kind, chatName, now)
	_ = a.DB().UpsertMessage(store.UpsertMessageParams{
		ChatJID:       to.String(),
		ChatName:      chatName,
		MsgID:         id,
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

	return id, map[string]string{
		"name":      name,
		"mime_type": mimeType,
		"media":     mediaType,
	}, nil
}

func chatKindFromJID(j types.JID) string {
	if j.Server == types.GroupServer {
		return "group"
	}
	if j.IsBroadcastList() {
		return "broadcast"
	}
	if j.Server == types.DefaultUserServer {
		return "dm"
	}
	return "unknown"
}
