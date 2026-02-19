package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steipete/wacli/internal/out"
	"github.com/steipete/wacli/internal/store"
	"github.com/steipete/wacli/internal/wa"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func newSendCmd(flags *rootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Send messages",
	}
	cmd.AddCommand(newSendTextCmd(flags))
	cmd.AddCommand(newSendFileCmd(flags))
	return cmd
}

func newSendTextCmd(flags *rootFlags) *cobra.Command {
	var to string
	var message string
	var replyTo string
	var replyToParticipant string

	cmd := &cobra.Command{
		Use:   "text",
		Short: "Send a text message",
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" || message == "" {
				return fmt.Errorf("--to and --message are required")
			}

			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()

			a, lk, err := newApp(ctx, flags, true, false)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			if err := a.EnsureAuthed(); err != nil {
				return err
			}
			if err := a.Connect(ctx, false, nil); err != nil {
				return err
			}

			toJID, err := wa.ParseUserOrJID(to)
			if err != nil {
				return err
			}

			var msgID string
			if replyTo != "" {
				// Resolve "self" to own JID.
				participant := strings.TrimSpace(replyToParticipant)
				if strings.EqualFold(participant, "self") {
					participant = a.WA().GetOwnJID().String()
				}

				msg := &waProto.Message{
					ExtendedTextMessage: &waProto.ExtendedTextMessage{
						Text: proto.String(message),
						ContextInfo: &waProto.ContextInfo{
							StanzaID:    proto.String(replyTo),
							Participant: proto.String(participant),
						},
					},
				}
				id, err := a.WA().SendProtoMessage(ctx, toJID, msg)
				if err != nil {
					return err
				}
				msgID = id
			} else {
				id, err := a.WA().SendText(ctx, toJID, message)
				if err != nil {
					return err
				}
				msgID = string(id)
			}

			now := time.Now().UTC()
			chat := toJID
			chatName := a.WA().ResolveChatName(ctx, chat, "")
			kind := chatKindFromJID(chat)
			_ = a.DB().UpsertChat(chat.String(), kind, chatName, now)
			_ = a.DB().UpsertMessage(store.UpsertMessageParams{
				ChatJID:    chat.String(),
				ChatName:   chatName,
				MsgID:      msgID,
				SenderJID:  "",
				SenderName: "me",
				Timestamp:  now,
				FromMe:     true,
				Text:       message,
			})

			if flags.asJSON {
				return out.WriteJSON(os.Stdout, map[string]any{
					"sent": true,
					"to":   chat.String(),
					"id":   msgID,
				})
			}
			fmt.Fprintf(os.Stdout, "Sent to %s (id %s)\n", chat.String(), msgID)
			return nil
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "recipient phone number or JID")
	cmd.Flags().StringVar(&message, "message", "", "message text")
	cmd.Flags().StringVar(&replyTo, "reply-to", "", "message ID to reply to (stanza ID)")
	cmd.Flags().StringVar(&replyToParticipant, "reply-to-participant", "", "JID of the sender of the quoted message (use 'self' for own messages)")
	return cmd
}
