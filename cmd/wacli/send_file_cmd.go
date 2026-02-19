package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/steipete/wacli/internal/app"
	"github.com/steipete/wacli/internal/config"
	"github.com/steipete/wacli/internal/out"
	"github.com/steipete/wacli/internal/wa"
)

func newSendFileCmd(flags *rootFlags) *cobra.Command {
	var to string
	var filePath string
	var filename string
	var caption string
	var mimeOverride string

	cmd := &cobra.Command{
		Use:   "file",
		Short: "Send a file (image/video/audio/document)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" || filePath == "" {
				return fmt.Errorf("--to and --file are required")
			}

			// Resolve store dir for socket path.
			storeDir := flags.storeDir
			if storeDir == "" {
				storeDir = config.DefaultStoreDir()
			}
			storeDir, _ = filepath.Abs(storeDir)

			// Try sending through the running sync process's socket first.
			sockPath := app.SendSocketPath(storeDir)
			if resp, err := trySendFileViaSocket(sockPath, to, filePath, filename, caption, mimeOverride); err == nil {
				if flags.asJSON {
					result := map[string]any{
						"sent": true,
						"to":   resp.To,
						"id":   resp.ID,
					}
					if resp.File != nil {
						result["file"] = resp.File
					}
					return out.WriteJSON(os.Stdout, result)
				}
				name := filename
				if name == "" {
					name = filepath.Base(filePath)
				}
				fmt.Fprintf(os.Stdout, "Sent %s to %s (id %s)\n", name, resp.To, resp.ID)
				return nil
			}

			// Fall back to direct connection (sync not running).
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

			msgID, meta, err := sendFile(ctx, a, toJID, filePath, filename, caption, mimeOverride)
			if err != nil {
				return err
			}

			if flags.asJSON {
				return out.WriteJSON(os.Stdout, map[string]any{
					"sent": true,
					"to":   toJID.String(),
					"id":   msgID,
					"file": meta,
				})
			}
			fmt.Fprintf(os.Stdout, "Sent %s to %s (id %s)\n", meta["name"], toJID.String(), msgID)
			return nil
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "recipient phone number or JID")
	cmd.Flags().StringVar(&filePath, "file", "", "path to file")
	cmd.Flags().StringVar(&filename, "filename", "", "display name for the file (defaults to basename of --file)")
	cmd.Flags().StringVar(&caption, "caption", "", "caption (images/videos/documents)")
	cmd.Flags().StringVar(&mimeOverride, "mime", "", "override detected mime type")
	return cmd
}

type sendFileSocketResponse struct {
	Success bool              `json:"success"`
	ID      string            `json:"id"`
	To      string            `json:"to"`
	Error   string            `json:"error"`
	File    map[string]string `json:"file"`
}

func trySendFileViaSocket(sockPath, to, filePath, filename, caption, mimeType string) (*sendFileSocketResponse, error) {
	conn, err := net.DialTimeout("unix", sockPath, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Minute))

	req, _ := json.Marshal(map[string]string{
		"to":        to,
		"type":      "file",
		"file_path": filePath,
		"filename":  filename,
		"caption":   caption,
		"mime":      mimeType,
	})
	conn.Write(req)
	conn.Write([]byte("\n"))

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return nil, fmt.Errorf("no response from send server")
	}

	var resp sendFileSocketResponse
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("send server error: %s", resp.Error)
	}
	return &resp, nil
}
